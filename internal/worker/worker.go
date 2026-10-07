package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

type Worker struct {
	pool            *pgxpool.Pool
	judge           *sandbox.Client
	consumer        *queue.DualConsumer
	pubsub          *queue.PubSub
	jobTimeout      time.Duration
	testParallelism int
	cc              *compileCache
}

func New(pool *pgxpool.Pool, judge *sandbox.Client, consumer *queue.DualConsumer, pubsub *queue.PubSub, jobTimeout time.Duration, testParallelism int, compileCacheTTL time.Duration) *Worker {
	return &Worker{
		pool:            pool,
		judge:           judge,
		consumer:        consumer,
		pubsub:          pubsub,
		jobTimeout:      jobTimeout,
		testParallelism: max(testParallelism, 1),
		cc:              newCompileCache(compileCacheTTL),
	}
}

// ProcessFunc returns the worker's process function for use by reclaimers and legacy drain.
func (w *Worker) ProcessFunc() func(ctx context.Context, submissionID string) error {
	return w.process
}

func (w *Worker) Start(ctx context.Context, n int, wg *sync.WaitGroup) {
	for i := range n {
		consumer := fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				panicked := w.runWorker(ctx, consumer)
				if !panicked || ctx.Err() != nil {
					return
				}
				slog.Warn("restarting worker after panic", "worker", consumer)
				time.Sleep(time.Second)
			}
		}()
	}
	if w.cc.ttl > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(w.cc.ttl)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					w.cc.evictExpired()
				}
			}
		}()
	}
	slog.Info("workers started", "count", n)
}

func (w *Worker) runWorker(ctx context.Context, consumer string) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic recovered", "worker", consumer, "panic", r)
			panicked = true
		}
	}()
	w.consumer.Consume(ctx, consumer, func(ctx context.Context, submissionID string) error {
		return w.process(ctx, submissionID)
	})
	return false
}

type testData struct {
	Input          string
	ExpectedOutput string
	IsSample       bool
}

func (w *Worker) process(ctx context.Context, submissionID string) error {
	start := time.Now()

	jobCtx, jobCancel := context.WithTimeout(ctx, w.jobTimeout)
	defer jobCancel()

	id, err := uuid.Parse(submissionID)
	if err != nil {
		return fmt.Errorf("worker: invalid UUID: %w", err)
	}

	log := slog.With("submission_id", submissionID)

	sub, err := db.GetSubmission(jobCtx, w.pool, id)
	if err != nil {
		return fmt.Errorf("worker: get submission: %w", err)
	}

	if sub.Status == model.StatusCompleted || sub.Status == model.StatusFailed {
		log.Info("skipping already-finished submission", "status", sub.Status)
		return nil
	}

	// Claim the submission early so orphan reaper won't re-enqueue it.
	if err := db.UpdateSubmissionStatus(jobCtx, w.pool, id, model.StatusRunning); err != nil {
		if strings.Contains(err.Error(), "already terminal") {
			log.Info("submission became terminal before claim", "submission_id", submissionID)
			return nil
		}
		return fmt.Errorf("worker: claim submission: %w", err)
	}

	// Count only jobs we've actually claimed; deferred duration also starts here.
	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()
	defer func() { metrics.JobProcessingDuration.Observe(time.Since(start).Seconds()) }()

	// Load tests from the appropriate source
	tests, limits, err := w.loadTests(jobCtx, sub)
	if err != nil {
		return w.failSubmission(jobCtx, id, model.VerdictInternalError, fmt.Sprintf("load tests: %v", err), log)
	}
	if len(tests) == 0 {
		return w.failSubmission(jobCtx, id, model.VerdictInternalError, "no tests found", log)
	}

	lang, ok := sandbox.Languages[sub.Language]
	if !ok {
		return w.failSubmission(jobCtx, id, model.VerdictInternalError, fmt.Sprintf("unknown language: %s", sub.Language), log)
	}

	// With batched execution the wall-clock budget is the slowest single test
	// (they run in parallel), not the sum. Keep the serial budget as a safe upper
	// bound so the context doesn't expire prematurely on large test sets.
	if budget := jobBudget(lang, limits, len(tests)); budget > w.jobTimeout {
		extendedCtx, extendedCancel := context.WithTimeout(ctx, budget)
		defer extendedCancel()
		jobCtx = extendedCtx
	}

	// Compile step (for compiled languages)
	var fileID string
	if lang.Compiled {
		if err := db.UpdateSubmissionStatus(jobCtx, w.pool, id, model.StatusCompiling); err != nil {
			if strings.Contains(err.Error(), "already terminal") {
				log.Info("submission became terminal before compile")
				return nil
			}
			return fmt.Errorf("worker: update status compiling: %w", err)
		}
		if err := w.pubsub.PublishEvent(jobCtx, eventlog.New(submissionID, eventlog.EventCompiling, nil)); err != nil {
			log.Warn("failed to publish COMPILING event", "err", err)
		}

		cacheKey := compileCacheKey(sub.Language, sub.SourceCode)
		if cached, hit := w.cc.get(cacheKey); hit {
			fileID = cached
			log.Info("compile cache hit", "fileId", fileID)
		} else {
			compileResult, err := w.compile(jobCtx, lang, sub.SourceCode)
			if err != nil {
				return w.failSubmission(jobCtx, id, model.VerdictInternalError, err.Error(), log)
			}

			if compileResult.Status != sandbox.StatusAccepted {
				stderr := compileResult.Files["stderr"]
				return w.failSubmission(jobCtx, id, model.VerdictCompilationError, stderr, log)
			}

			for _, fid := range compileResult.FileIDs {
				fileID = fid
				break
			}
			if fileID == "" {
				return w.failSubmission(jobCtx, id, model.VerdictInternalError, "no cached file returned from compile", log)
			}
			w.cc.put(cacheKey, fileID)
			log.Info("compiled", "fileId", fileID)
		}
	}

	// Run tests — only update status if we went through the compile phase
	// (interpreted languages are already in StatusRunning from the claim step).
	if lang.Compiled {
		if err := db.UpdateSubmissionStatus(jobCtx, w.pool, id, model.StatusRunning); err != nil {
			if strings.Contains(err.Error(), "already terminal") {
				log.Info("submission became terminal before test run")
				return nil
			}
			return fmt.Errorf("worker: update status running: %w", err)
		}
	}
	if err := w.pubsub.PublishEvent(jobCtx, eventlog.New(submissionID, eventlog.EventRunning, nil)); err != nil {
		log.Warn("failed to publish RUNNING event", "err", err)
	}

	results, totalCPU, peakMem, passed, overallVerdict, err := w.runTestsBatched(jobCtx, submissionID, lang, sub.SourceCode, tests, fileID, limits, log)
	if err != nil {
		return w.failSubmission(jobCtx, id, model.VerdictInternalError, fmt.Sprintf("batch run: %v", err), log)
	}

	// writeCtx survives signal cancellation so DB writes complete during graceful shutdown.
	// Created here (after sandbox execution) so the 15s budget covers only the write phase.
	writeCtx, writeCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer writeCancel()

	if err := db.InsertResultsAndVerdict(writeCtx, w.pool, id, results,
		model.StatusCompleted, overallVerdict, "",
		totalCPU, peakMem, len(tests), passed); err != nil {
		if strings.Contains(err.Error(), "verdict already written") {
			log.Warn("duplicate execution detected, verdict already persisted")
			return nil
		}
		return fmt.Errorf("worker: write results+verdict: %w", err)
	}

	// Build verdict event data — filter hidden test output for submit mode
	eventResults := make([]map[string]any, len(results))
	isSubmitMode := sub.Mode == string(model.ModeSubmit)
	for i, r := range results {
		entry := map[string]any{
			"test_index":   r.TestIndex,
			"verdict":      r.Verdict,
			"cpu_time_ns":  r.CPUTimeNs,
			"memory_bytes": r.MemoryBytes,
		}
		// Only include stdout/stderr for sample tests in submit mode, or always in run mode
		if !isSubmitMode || (i < len(tests) && tests[i].IsSample) {
			entry["stdout_preview"] = db.Truncate(r.StdoutPreview, 256)
			entry["stderr_preview"] = db.Truncate(r.StderrPreview, 256)
		}
		// Run-mode callers (an exam server grading with its own checker) need the
		// complete output, not a preview; it is bounded by the sandbox output limit.
		if !isSubmitMode {
			entry["stdout"] = r.StdoutPreview
			entry["stderr"] = r.StderrPreview
		}
		eventResults[i] = entry
	}

	verdictData := map[string]any{
		"verdict":      overallVerdict,
		"tests_passed": passed,
		"test_count":   len(tests),
		"cpu_time_ns":  totalCPU,
		"memory_bytes": peakMem,
		"results":      eventResults,
	}
	if err := w.pubsub.PublishEvent(writeCtx, eventlog.New(submissionID, eventlog.EventVerdict, verdictData)); err != nil {
		log.Error("failed to publish VERDICT event (DB has verdict, SSE client may not)", "err", err)
	}

	metrics.JobsProcessedTotal.WithLabelValues(string(overallVerdict)).Inc()

	log.Info("completed", "verdict", overallVerdict, "passed", passed, "total", len(tests))
	return nil
}

func (w *Worker) loadTests(ctx context.Context, sub *model.Submission) ([]testData, sandbox.RunLimits, error) {
	if sub.Mode == string(model.ModeSubmit) && sub.ProblemVersionID != nil {
		tcs, err := db.GetTestCases(ctx, w.pool, *sub.ProblemVersionID)
		if err != nil {
			return nil, sandbox.RunLimits{}, err
		}
		tests := make([]testData, len(tcs))
		for i, tc := range tcs {
			tests[i] = testData{
				Input:          tc.Input,
				ExpectedOutput: tc.ExpectedOutput,
				IsSample:       tc.IsSample,
			}
		}
		return tests, sandbox.RunLimits{}, nil
	}

	// Run mode: tests and limits from Redis
	redisTests, limits, err := w.pubsub.GetTests(ctx, sub.ID.String())
	if err != nil {
		return nil, sandbox.RunLimits{}, err
	}
	tests := make([]testData, len(redisTests))
	for i, rt := range redisTests {
		tests[i] = testData{
			Input:          rt.Input,
			ExpectedOutput: rt.ExpectedOutput,
			IsSample:       true, // all client-supplied tests are visible
		}
	}
	return tests, limits, nil
}

// jobBudget is the worst-case time a job needs: compiling plus every test
// running to its wall-clock limit, with slack for sandbox and DB round trips.
func jobBudget(lang sandbox.LangConfig, limits sandbox.RunLimits, tests int) time.Duration {
	cpu, _ := lang.Effective(limits)
	total := sandbox.ClockLimit(cpu) * uint64(tests)
	if lang.Compiled {
		total += sandbox.ClockLimit(lang.CompileCPU)
	}
	return time.Duration(total) + 15*time.Second
}

func (w *Worker) compile(ctx context.Context, lang sandbox.LangConfig, sourceCode string) (*sandbox.Result, error) {
	cmd := sandbox.BuildCompileCmd(lang, sourceCode)
	results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{cmd}})
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("compile: empty response")
	}
	return &results[0], nil
}

// maxBatchSize caps the number of test Cmds sent in a single go-judge /run call.
// go-judge runs them in parallel up to its -parallelism flag; sending too many
// in one request risks OOM or long response times. Excess tests are sent in
// subsequent batches.
const maxBatchSize = 32

// runTestsBatched sends all tests to go-judge in batched /run calls instead of
// one HTTP round-trip per test. go-judge executes the Cmds in the batch in
// parallel (up to its -parallelism setting), so a 10-test submission that took
// 10 sequential round-trips now completes in ~1 round-trip wall-clock time.
func (w *Worker) runTestsBatched(ctx context.Context, jobID string, lang sandbox.LangConfig, sourceCode string, tests []testData, fileID string, limits sandbox.RunLimits, log *slog.Logger) (results []model.TestResult, totalCPU, peakMem int64, passed int, overallVerdict model.Verdict, err error) {
	overallVerdict = model.VerdictAccepted
	results = make([]model.TestResult, 0, len(tests))

	// For interpreted languages with multiple tests, pre-cache the source file
	// so each test Cmd uses a fileID reference instead of embedding the full source.
	if !lang.Compiled && !lang.StdinIsSource && fileID == "" && len(tests) > 1 {
		cacheCmd := sandbox.BuildCacheSourceCmd(lang, sourceCode)
		cacheResults, cacheErr := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{cacheCmd}})
		if cacheErr == nil && len(cacheResults) == 1 && cacheResults[0].Status == sandbox.StatusAccepted {
			for _, fid := range cacheResults[0].FileIDs {
				fileID = fid
				break
			}
			if fileID != "" {
				log.Debug("cached interpreted source", "fileId", fileID)
			}
		}
		// Non-fatal: falls back to inlining source in each Cmd
	}

	for batchStart := 0; batchStart < len(tests); batchStart += maxBatchSize {
		if ctx.Err() != nil {
			log.Warn("job context expired, skipping remaining tests", "completed", batchStart, "total", len(tests))
			for j := batchStart; j < len(tests); j++ {
				results = append(results, model.TestResult{TestIndex: j, Verdict: model.VerdictInternalError})
			}
			if overallVerdict == model.VerdictAccepted {
				overallVerdict = model.VerdictInternalError
			}
			return results, totalCPU, peakMem, passed, overallVerdict, nil
		}

		batchEnd := min(batchStart+maxBatchSize, len(tests))
		batch := tests[batchStart:batchEnd]

		cmds := make([]sandbox.Cmd, len(batch))
		for i, tc := range batch {
			cmds[i] = sandbox.BuildRunCmd(lang, sourceCode, tc.Input, fileID, limits)
		}

		batchResults, runErr := w.judge.Run(ctx, sandbox.Request{Cmd: cmds})
		if runErr != nil {
			return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("batch run (tests %d-%d): %w", batchStart, batchEnd-1, runErr)
		}
		if len(batchResults) != len(batch) {
			return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("batch run: expected %d results, got %d", len(batch), len(batchResults))
		}

		for i, r := range batchResults {
			testIndex := batchStart + i
			tr := judgeResult(r, testIndex, batch[i].ExpectedOutput)
			results = append(results, tr)
			totalCPU += tr.CPUTimeNs
			if tr.MemoryBytes > peakMem {
				peakMem = tr.MemoryBytes
			}
			if tr.Verdict == model.VerdictAccepted {
				passed++
			} else if overallVerdict == model.VerdictAccepted {
				overallVerdict = tr.Verdict
			}
		}

		if err := w.pubsub.PublishEvent(ctx, eventlog.New(jobID, eventlog.EventTestResult, map[string]any{
			"completed":  batchEnd,
			"total":      len(tests),
			"passed":     passed,
			"batch_from": batchStart,
			"batch_to":   batchEnd - 1,
		})); err != nil {
			log.Warn("failed to publish TEST_RESULT event", "err", err)
		}
	}

	return results, totalCPU, peakMem, passed, overallVerdict, nil
}

// judgeResult converts a single go-judge sandbox.Result into a model.TestResult.
func judgeResult(r sandbox.Result, testIndex int, expectedOutput string) model.TestResult {
	tr := model.TestResult{
		TestIndex:     testIndex,
		CPUTimeNs:     int64(r.Time),   //nolint:gosec // go-judge reports CPU time in ns (non-negative)
		MemoryBytes:   int64(r.Memory), //nolint:gosec // go-judge reports memory usage in bytes (non-negative)
		StdoutPreview: r.Files["stdout"],
		StderrPreview: r.Files["stderr"],
	}

	switch r.Status {
	case sandbox.StatusAccepted:
		tr.Verdict = JudgeOutput(expectedOutput, r.Files["stdout"])
	case sandbox.StatusTimeLimitEx:
		tr.Verdict = model.VerdictTimeLimitEx
	case sandbox.StatusMemoryLimitEx:
		tr.Verdict = model.VerdictMemoryLimitEx
	case sandbox.StatusOutputLimitEx:
		tr.Verdict = model.VerdictOutputLimitEx
	case sandbox.StatusNonzeroExit, sandbox.StatusSignalled:
		tr.Verdict = model.VerdictRuntimeError
	default:
		tr.Verdict = model.VerdictInternalError
	}

	return tr
}

const maxStderrBytes = 4096

func (w *Worker) failSubmission(ctx context.Context, id uuid.UUID, verdict model.Verdict, stderr string, log *slog.Logger) error {
	stderr = db.Truncate(stderr, maxStderrBytes)

	// Detached context so the DB write completes even during graceful shutdown.
	writeCtx, writeCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer writeCancel()

	if err := db.UpdateSubmissionVerdict(writeCtx, w.pool, id, model.StatusFailed, verdict, stderr, 0, 0, 0, 0); err != nil {
		if strings.Contains(err.Error(), "verdict already written") {
			log.Warn("duplicate execution detected in failSubmission, verdict already persisted")
			return nil
		}
		return fmt.Errorf("worker: failSubmission update verdict: %w", err)
	}

	verdictData := map[string]any{
		"verdict":        verdict,
		"compile_stderr": stderr,
	}
	if err := w.pubsub.PublishEvent(writeCtx, eventlog.New(id.String(), eventlog.EventVerdict, verdictData)); err != nil {
		log.Error("failed to publish VERDICT event for failed submission", "err", err)
	}

	metrics.JobsProcessedTotal.WithLabelValues(string(verdict)).Inc()

	log.Info("submission failed", "verdict", verdict)
	return nil
}
