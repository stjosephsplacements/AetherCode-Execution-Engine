package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/auth"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/config"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/gateway"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/ratelimit"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/worker"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/migrations"
)

func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	var logHandler slog.Handler
	logOpts := &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)}
	if cfg.LogFormat == "text" {
		logHandler = slog.NewTextHandler(os.Stderr, logOpts)
	} else {
		logHandler = slog.NewJSONHandler(os.Stderr, logOpts)
	}
	slog.SetDefault(slog.New(logHandler))

	// Apply Go runtime tuning before any goroutines are spawned.
	prev := debug.SetGCPercent(cfg.GOGC)
	slog.Debug("gc tuning", "GOGC", cfg.GOGC, "previous", prev)
	if cfg.GOMEMLIMIT > 0 {
		debug.SetMemoryLimit(cfg.GOMEMLIMIT)
		slog.Debug("gc tuning", "GOMEMLIMIT", cfg.GOMEMLIMIT)
	}

	slog.Info("aethercode-exec starting",
		"gateway", cfg.GatewayAddr,
		"workers", cfg.WorkerCount,
		"job_timeout", cfg.JobTimeout,
		"log_format", cfg.LogFormat,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Sandbox client
	judge := sandbox.NewClient(cfg.JudgeURL, cfg.JudgeToken)
	if err := judge.Ping(ctx); err != nil {
		slog.Error("go-judge not reachable", "url", cfg.JudgeURL, "err", err)
		os.Exit(1)
	}
	slog.Info("go-judge connected", "url", cfg.JudgeURL)

	// Database
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database connection failed", "err", err)
		os.Exit(1)
	}
	slog.Info("database connected")

	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		PoolSize:     256,
		MinIdleConns: 32,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("redis connection failed", "addr", cfg.RedisAddr, "err", err)
		os.Exit(1)
	}
	slog.Info("redis connected", "addr", cfg.RedisAddr)

	submitStream := queue.NewStream(rdb, cfg.SubmitStreamName, cfg.ConsumerGroup)
	runStream := queue.NewStream(rdb, cfg.RunStreamName, cfg.ConsumerGroup)

	dualConsumer := queue.NewDualConsumer(rdb, cfg.SubmitStreamName, cfg.RunStreamName, cfg.ConsumerGroup, cfg.SubmitWeight, cfg.BlockReadTimeout)
	if err := dualConsumer.CreateGroups(ctx); err != nil {
		slog.Error("redis stream group creation failed", "err", err)
		os.Exit(1)
	}
	slog.Info("redis streams ready",
		"submit", cfg.SubmitStreamName, "run", cfg.RunStreamName,
		"group", cfg.ConsumerGroup, "weight", cfg.SubmitWeight)

	pubsub := queue.NewPubSub(rdb)

	// Workers (dual-consumer weighted fair queuing)
	var workerWg sync.WaitGroup
	w := worker.New(pool, judge, dualConsumer, pubsub, cfg.JobTimeout, cfg.TestParallelism, cfg.CompileCacheTTL)
	w.Start(ctx, cfg.WorkerCount, &workerWg)

	// Reclaimer — picks up stuck messages every 30s
	dualConsumer.StartReclaimer(ctx, &workerWg, w.ProcessFunc())

	// Drain legacy single stream if it has remaining messages
	queue.DrainLegacyStream(ctx, &workerWg, rdb, cfg.StreamName, cfg.ConsumerGroup, w.ProcessFunc())

	// Background goroutines tracked so shutdown waits for them before pool.Close().
	var bgWg sync.WaitGroup

	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		scrapeQueueDepth(ctx, rdb, cfg.SubmitStreamName, cfg.RunStreamName)
	}()

	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		reapOrphans(ctx, pool, submitStream, runStream)
	}()

	// Rate limiter
	rl := ratelimit.New(rdb, cfg.RateLimitPerMinute, cfg.RateLimitBurst)

	// HTTP Gateway
	mux := http.NewServeMux()
	h := gateway.NewHandler(pool, submitStream, runStream, pubsub, judge, rdb, rl, cfg.AdmissionMaxQueueDepth)
	h.Register(mux)

	var handler http.Handler = mux

	// Auth (innermost — runs closest to the handler)
	if cfg.AuthEnabled {
		authMw, err := auth.NewMiddleware(cfg.OIDCIssuer, cfg.OIDCJWKSURL, cfg.OIDCAudience, pool)
		if err != nil {
			slog.Error("auth middleware init failed", "err", err)
			os.Exit(1)
		}
		defer authMw.Close()
		handler = authMw.Wrap(handler)
		slog.Info("auth enabled", "issuer", cfg.OIDCIssuer)
	}

	// CORS (must be outside auth so OPTIONS preflight works without a token)
	cors := gateway.NewCORS(cfg.CORSAllowedOrigins)
	handler = cors.Wrap(handler)

	// Request logging (outermost — captures everything including CORS and auth)
	handler = gateway.LoggingMiddleware(handler)

	srv := &http.Server{
		Addr:              cfg.GatewayAddr,
		Handler:           handler,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      0, // SSE streams are long-lived
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		slog.Info("gateway listening", "addr", cfg.GatewayAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("gateway error", "err", err)
			stop() // cancel the root context, triggering graceful shutdown
		}
	}()

	// Debug/pprof server (localhost only)
	debugMux := http.NewServeMux()
	debugMux.HandleFunc("/debug/pprof/", pprof.Index)
	debugMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	debugMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	debugMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	debugMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	debugMux.Handle("/metrics", promhttp.Handler())
	debugSrv := &http.Server{
		Addr:              "127.0.0.1:6060",
		Handler:           debugMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		slog.Info("debug/pprof listening", "addr", "127.0.0.1:6060")
		if err := debugSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("debug server error", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	// Phase 1: Stop workers from picking new jobs. Workers already processing
	// will finish, publish VERDICTs, and SSE clients will receive them.
	// ctx is already cancelled — workers' Consume loops will exit.
	slog.Info("waiting for in-flight jobs to complete")
	workerDone := make(chan struct{})
	go func() {
		workerWg.Wait()
		close(workerDone)
	}()

	// Give workers up to 120s to drain (jobTimeout 90s + writeCtx 15s + margin)
	const workerDrainTimeout = 120 * time.Second
	workerDeadline := time.NewTimer(workerDrainTimeout)
	select {
	case <-workerDone:
		slog.Info("all workers stopped")
	case <-workerDeadline.C:
		slog.Warn("worker drain timeout, proceeding with shutdown")
	}
	workerDeadline.Stop()

	// Wait for background goroutines (scraper, reaper) — they exit quickly
	// once ctx is cancelled, but we must wait before pool.Close().
	bgWg.Wait()

	// Phase 2: Shut down HTTP servers. In-flight SSE connections get a
	// brief window to flush their final events before close.
	debugCtx, debugCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer debugCancel()
	debugSrv.Shutdown(debugCtx) //nolint:errcheck // best-effort shutdown

	srvCtx, srvCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer srvCancel()
	srv.Shutdown(srvCtx) //nolint:errcheck // best-effort shutdown

	pubsub.Wait() // drain Subscribe goroutines before rdb.Close()

	// Explicit ordered resource teardown: all consumers are gone before their pools close.
	rdb.Close()  //nolint:errcheck // best-effort: all consumers drained, so a close error is non-actionable at shutdown
	pool.Close() //nolint:errcheck // best-effort: all consumers drained, so a close error is non-actionable at shutdown
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func reapOrphans(ctx context.Context, pool *pgxpool.Pool, submitStream, runStream *queue.Stream) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			orphans, err := db.OrphanedSubmissions(ctx, pool)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("orphan reaper: query failed", "err", err)
				}
				continue
			}
			for _, sub := range orphans {
				target := runStream
				if sub.Mode == "submit" {
					target = submitStream
				}
				if err := target.Enqueue(ctx, sub.ID.String()); err != nil {
					slog.Error("orphan reaper: re-enqueue failed", "submission_id", sub.ID, "err", err)
					continue
				}
				if err := db.TouchSubmissionUpdatedAt(ctx, pool, sub.ID); err != nil {
					slog.Error("orphan reaper: touch updated_at failed", "submission_id", sub.ID, "err", err)
				}
				slog.Warn("orphan reaper: re-enqueued stuck submission", "submission_id", sub.ID, "mode", sub.Mode)
			}
		}
	}
}

func scrapeQueueDepth(ctx context.Context, rdb *redis.Client, submitStream, runStream string) {
	streams := []string{submitStream, runStream}
	scrape := func() {
		for _, stream := range streams {
			length, err := rdb.XLen(ctx, stream).Result()
			if err == nil {
				metrics.QueueDepth.WithLabelValues(stream).Set(float64(length))
			}
		}
	}
	scrape() // immediate first reading
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrape()
		}
	}
}
