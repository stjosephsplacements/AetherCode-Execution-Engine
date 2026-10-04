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
	pool       *pgxpool.Pool
	judge      *sandbox.Client
	consumer   *queue.DualConsumer
	pubsub     *queue.PubSub
	jobTimeout time.Duration
}

func New(pool *pgxpool.Pool, judge *sandbox.Client, consumer *queue.DualConsumer, pubsub *queue.PubSub, jobTimeout time.Duration) *Worker {
	return &Worker{
		pool:       pool,
		judge:      judge,
		consumer:   consumer,
		pubsub:     pubsub,
		jobTimeout: jobTimeout,
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
	tests, err := w.loadTests(jobCtx, sub)
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
		log.Info("compiled", "fileId", fileID)
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

	var results []model.TestResult
	var totalCPU, peakMem int64
	overallVerdict := model.VerdictAccepted
	passed := 0

	for i, tc := range tests {
		if jobCtx.Err() != nil {
			log.Warn("job context expired, skipping remaining tests", "completed", i, "total", len(tests))
			for j := i; j < len(tests); j++ {
				results = append(results, model.TestResult{
					TestIndex: j,
					Verdict:   model.VerdictInternalError,
				})
			}
			if overallVerdict == model.VerdictAccepted {
				overallVerdict = model.VerdictInternalError
			}
			break
		}

		tr, err := w.runTest(jobCtx, lang, sub.SourceCode, tc, fileID, i)
		if err != nil {
			log.Error("test execution error", "test", i, "err", err)
			tr = model.TestResult{
				TestIndex: i,
				Verdict:   model.VerdictInternalError,
			}
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

	// writeCtx survives signal cancellation so DB writes complete during graceful shutdown.
	// Created here (after sandbox execution) so the 15s budget covers only the write phase.
	writeCtx, writeCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer writeCancel()

	if err := db.InsertTestResults(writeCtx, w.pool, id, results); err != nil {
		return fmt.Errorf("worker: insert test results: %w", err)
	}

	if err := db.UpdateSubmissionVerdict(writeCtx, w.pool, id,
		model.StatusCompleted, overallVerdict, "",
		totalCPU, peakMem, len(tests), passed); err != nil {
		if strings.Contains(err.Error(), "verdict already written") {
			log.Warn("duplicate execution detected, verdict already persisted")
			return nil
		}
		return fmt.Errorf("worker: update verdict: %w", err)
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

func (w *Worker) loadTests(ctx context.Context, sub *model.Submission) ([]testData, error) {
	if sub.Mode == string(model.ModeSubmit) && sub.ProblemVersionID != nil {
		tcs, err := db.GetTestCases(ctx, w.pool, *sub.ProblemVersionID)
		if err != nil {
			return nil, err
		}
		tests := make([]testData, len(tcs))
		for i, tc := range tcs {
			tests[i] = testData{
				Input:          tc.Input,
				ExpectedOutput: tc.ExpectedOutput,
				IsSample:       tc.IsSample,
			}
		}
		return tests, nil
	}

	// Run mode: tests from Redis
	redisTests, err := w.pubsub.GetTests(ctx, sub.ID.String())
	if err != nil {
		return nil, err
	}
	tests := make([]testData, len(redisTests))
	for i, rt := range redisTests {
		tests[i] = testData{
			Input:          rt.Input,
			ExpectedOutput: rt.ExpectedOutput,
			IsSample:       true, // all client-supplied tests are visible
		}
	}
	return tests, nil
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

func (w *Worker) runTest(ctx context.Context, lang sandbox.LangConfig, sourceCode string, tc testData, fileID string, testIndex int) (model.TestResult, error) {
	cmd := sandbox.BuildRunCmd(lang, sourceCode, tc.Input, fileID)
	results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{cmd}})
	if err != nil {
		return model.TestResult{TestIndex: testIndex, Verdict: model.VerdictInternalError}, err
	}
	if len(results) == 0 {
		return model.TestResult{TestIndex: testIndex, Verdict: model.VerdictInternalError}, fmt.Errorf("empty response")
	}

	r := results[0]
	tr := model.TestResult{
		TestIndex:     testIndex,
		CPUTimeNs:     int64(r.Time), //nolint:gosec // go-judge reports CPU time in ns (non-negative)
		MemoryBytes:   int64(r.Memory), //nolint:gosec // go-judge reports memory usage in bytes (non-negative)
		StdoutPreview: r.Files["stdout"],
		StderrPreview: r.Files["stderr"],
	}

	switch r.Status {
	case sandbox.StatusAccepted:
		tr.Verdict = JudgeOutput(tc.ExpectedOutput, r.Files["stdout"])
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

	return tr, nil
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
