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
	Input              string
	ExpectedOutput     string
	CheckerType        string // "exact" | "token" | "float" | "custom"; empty means exact
	CheckerCode        string
	CheckerLanguage    string
	InteractorCode     string
	InteractorLanguage string
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
	// finalVerdict is set just before each return so the deferred histogram gets labels.
	finalVerdict := string(model.VerdictInternalError)
	defer func() {
		metrics.JobProcessingDuration.WithLabelValues(sub.Language, finalVerdict).Observe(time.Since(start).Seconds())
	}()

	// Heartbeat — touch updated_at every 30s so the orphan reaper doesn't
	// re-enqueue a legitimately running job before its timeout expires.
	heartbeatCtx, heartbeatCancel := context.WithCancel(jobCtx)
	defer heartbeatCancel()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				db.TouchSubmissionUpdatedAt(heartbeatCtx, w.pool, id) //nolint:errcheck
			}
		}
	}()

	// Load the job's tests and limits from Redis
	tests, limits, extraFiles, compilerFlags, err := w.loadTests(jobCtx, submissionID)
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

		cacheKey := compileCacheKeyWithOptions(sub.Language, sub.SourceCode, extraFiles, compilerFlags)
		if cached, hit := w.cc.get(cacheKey); hit {
			fileID = cached
			metrics.CompileCacheHitsTotal.Inc()
			log.Info("compile cache hit", "fileId", fileID)
		} else {
			compileStart := time.Now()
			compileResult, err := w.compileWithOptions(jobCtx, lang, sub.SourceCode, extraFiles, compilerFlags)
			metrics.CompileDuration.WithLabelValues(sub.Language).Observe(time.Since(compileStart).Seconds())
			if err != nil {
				return w.failSubmission(jobCtx, id, model.VerdictInternalError, err.Error(), log)
			}

			if compileResult.Status != sandbox.StatusAccepted {
				stderr := compileResult.Files["stderr"]
				finalVerdict = string(model.VerdictCompilationError)
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
			metrics.CompileCacheMissesTotal.Inc()
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

	results, totalCPU, peakMem, passed, overallVerdict, err := w.runTestsBatched(jobCtx, submissionID, lang, sub.SourceCode, tests, fileID, limits, extraFiles, log)
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

	eventResults := verdictResults(results)

	effectiveCPU, effectiveMem := lang.Effective(limits)
	verdictData := map[string]any{
		"verdict":                    overallVerdict,
		"tests_passed":               passed,
		"test_count":                 len(tests),
		"cpu_time_ns":                totalCPU,
		"memory_bytes":               peakMem,
		"applied_cpu_limit_ns":       effectiveCPU,
		"applied_memory_limit_bytes": effectiveMem,
		"results":                    eventResults,
	}
	if err := w.pubsub.PublishEvent(writeCtx, eventlog.New(submissionID, eventlog.EventVerdict, verdictData)); err != nil {
		log.Error("failed to publish VERDICT event (DB has verdict, SSE client may not)", "err", err)
	}

	finalVerdict = string(overallVerdict)
	metrics.JobsProcessedTotal.WithLabelValues(string(overallVerdict)).Inc()

	log.Info("completed", "verdict", overallVerdict, "passed", passed, "total", len(tests))
	return nil
}

func (w *Worker) loadTests(ctx context.Context, jobID string) ([]testData, sandbox.RunLimits, map[string]string, []string, error) {
	set, err := w.pubsub.GetTests(ctx, jobID)
	if err != nil {
		return nil, sandbox.RunLimits{}, nil, nil, err
	}
	tests := make([]testData, len(set.Tests))
	for i, rt := range set.Tests {
		tests[i] = testData{
			Input:              rt.Input,
			ExpectedOutput:     rt.ExpectedOutput,
			CheckerType:        set.Checker,
			CheckerCode:        set.CheckerCode,
			CheckerLanguage:    set.CheckerLanguage,
			InteractorCode:     set.InteractorCode,
			InteractorLanguage: set.InteractorLanguage,
		}
	}
	return tests, set.Limits, set.Files, set.CompilerFlags, nil
}

// verdictResults builds the per-test entries of the VERDICT event. The caller
// supplied the tests, so every test carries its full output (bounded by the
// sandbox output limit) plus a 256-byte preview.
func verdictResults(results []model.TestResult) []map[string]any {
	out := make([]map[string]any, len(results))
	for i, r := range results {
		out[i] = map[string]any{
			"test_index":     r.TestIndex,
			"verdict":        r.Verdict,
			"cpu_time_ns":    r.CPUTimeNs,
			"memory_bytes":   r.MemoryBytes,
			"stdout_preview": db.Truncate(r.StdoutPreview, 256),
			"stderr_preview": db.Truncate(r.StderrPreview, 256),
			"stdout":         r.StdoutPreview,
			"stderr":         r.StderrPreview,
		}
	}
	return out
}

// jobBudget is the worst-case time a job needs: compiling plus every test
// running to its wall-clock limit, with slack for sandbox and DB round trips.
func jobBudget(lang sandbox.LangConfig, limits sandbox.RunLimits, tests int) time.Duration {
	cpu, _ := lang.Effective(limits)
	total := sandbox.ClockLimit(cpu) * uint64(tests) //nolint:gosec // G115: tests is a non-negative test count
	if lang.Compiled {
		total += sandbox.ClockLimit(lang.CompileCPU)
	}
	return time.Duration(total) + 15*time.Second //nolint:gosec // G115: total is a bounded ns budget, far below MaxInt64
}

func (w *Worker) compileWithOptions(ctx context.Context, lang sandbox.LangConfig, sourceCode string, files map[string]string, compilerFlags []string) (*sandbox.Result, error) {
	cmd := sandbox.BuildCompileCmdWithOptions(lang, sourceCode, files, compilerFlags)
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
func (w *Worker) runTestsBatched(ctx context.Context, jobID string, lang sandbox.LangConfig, sourceCode string, tests []testData, fileID string, limits sandbox.RunLimits, extraFiles map[string]string, log *slog.Logger) (results []model.TestResult, totalCPU, peakMem int64, passed int, overallVerdict model.Verdict, err error) {
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

	// If custom checker is requested, prepare it (compile/cache) before running tests.
	var custom *customChecker
	if len(tests) > 0 && tests[0].CheckerType == "custom" && tests[0].CheckerCode != "" {
		c, err := w.prepareCustomChecker(ctx, tests[0].CheckerLanguage, tests[0].CheckerCode, log)
		if err != nil {
			return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("custom checker setup: %w", err)
		}
		custom = c
	}

	// If interactive interactor is provided, prepare it before running tests.
	var it *interactor
	if len(tests) > 0 && tests[0].InteractorCode != "" {
		prepIt, err := w.prepareInteractor(ctx, tests[0].InteractorLanguage, tests[0].InteractorCode, log)
		if err != nil {
			return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("interactor setup: %w", err)
		}
		it = prepIt
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

		if it != nil {
			// For interactive runs, execute one-by-one with bidirectional pipeMapping
			for i, tc := range batch {
				interactiveCmds, pipeMapping := buildInteractiveCmds(lang, sourceCode, fileID, limits, extraFiles, it, tc.Input, tc.ExpectedOutput)
				runResults, runErr := w.judge.Run(ctx, sandbox.Request{
					Cmd:         interactiveCmds,
					PipeMapping: pipeMapping,
				})
				testIndex := batchStart + i
				if runErr != nil || len(runResults) < 2 {
					tr := model.TestResult{TestIndex: testIndex, Verdict: model.VerdictInternalError}
					results = append(results, tr)
					if overallVerdict == model.VerdictAccepted {
						overallVerdict = model.VerdictInternalError
					}
					continue
				}

				studentRes := runResults[0]
				interactorRes := runResults[1]

				tr := model.TestResult{
					TestIndex:     testIndex,
					CPUTimeNs:     int64(studentRes.Time),   //nolint:gosec // go-judge reports CPU time in ns
					MemoryBytes:   int64(studentRes.Memory), //nolint:gosec // go-judge reports memory in bytes
					StdoutPreview: studentRes.Files["stdout"],
					StderrPreview: studentRes.Files["stderr"],
				}

				switch studentRes.Status {
				case sandbox.StatusAccepted:
					// Check interactor status and exit code
					if interactorRes.Status == sandbox.StatusAccepted && interactorRes.ExitStatus == 0 {
						tr.Verdict = model.VerdictAccepted
					} else {
						tr.Verdict = model.VerdictWrongAnswer
						tr.StderrPreview = interactorRes.Files["stderr"]
					}
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
		} else {
			cmds := make([]sandbox.Cmd, len(batch))
			for i, tc := range batch {
				cmds[i] = sandbox.BuildRunCmdWithOptions(lang, sourceCode, tc.Input, fileID, limits, extraFiles)
			}

			batchResults, runErr := w.judge.Run(ctx, sandbox.Request{Cmd: cmds})
			if runErr != nil {
				return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("batch run (tests %d-%d): %w", batchStart, batchEnd-1, runErr)
			}
			if len(batchResults) != len(batch) {
				return nil, 0, 0, 0, model.VerdictInternalError, fmt.Errorf("batch run: expected %d results, got %d", len(batch), len(batchResults))
			}

			for i := range batch {
				r := batchResults[i]
				testIndex := batchStart + i
				tc := batch[i]
				tr := judgeResult(r, testIndex, tc.ExpectedOutput, tc.CheckerType)
				if tr.Verdict == model.VerdictAccepted && custom != nil {
					// Run custom checker against input, expected, and actual stdout
					cVerdict, _ := w.evaluateCustomChecker(ctx, custom, tc.Input, tc.ExpectedOutput, r.Files["stdout"])
					tr.Verdict = cVerdict
				}
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
		}

		if w.pubsub != nil {
			if err := w.pubsub.PublishEvent(ctx, eventlog.New(jobID, eventlog.EventTestResult, map[string]any{
				"completed":  batchEnd,
				"total":      len(tests),
				"passed":     passed,
				"batch_from": batchStart,
				"batch_to":   batchEnd - 1,
			})); err != nil && log != nil {
				log.Warn("failed to publish TEST_RESULT event", "err", err)
			}
		}
	}

	return results, totalCPU, peakMem, passed, overallVerdict, nil
}

// judgeResult converts a single go-judge sandbox.Result into a model.TestResult.
func judgeResult(r sandbox.Result, testIndex int, expectedOutput, checkerType string) model.TestResult {
	tr := model.TestResult{
		TestIndex:     testIndex,
		CPUTimeNs:     int64(r.Time),   //nolint:gosec // go-judge reports CPU time in ns (non-negative)
		MemoryBytes:   int64(r.Memory), //nolint:gosec // go-judge reports memory usage in bytes (non-negative)
		StdoutPreview: r.Files["stdout"],
		StderrPreview: r.Files["stderr"],
	}

	switch r.Status {
	case sandbox.StatusAccepted:
		tr.Verdict = JudgeOutput(checkerType, expectedOutput, r.Files["stdout"])
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
