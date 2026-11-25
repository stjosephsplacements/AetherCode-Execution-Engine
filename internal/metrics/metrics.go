package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "http_requests_total",
		Help:      "Total HTTP requests by method, path, and status code.",
	}, []string{"method", "path", "code"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request latency in seconds.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"method", "path"})

	JobsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "jobs_processed_total",
		Help:      "Total jobs processed by verdict.",
	}, []string{"verdict"})

	JobProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "job_processing_duration_seconds",
		Help:      "Time from dequeue to verdict in seconds.",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90},
	}, []string{"language", "verdict"})

	CompileDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "compile_duration_seconds",
		Help:      "Time spent in the compilation step per language.",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90},
	}, []string{"language"})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ac",
		Name:      "active_workers",
		Help:      "Number of workers currently processing a job.",
	})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ac",
		Name:      "queue_depth",
		Help:      "Approximate queue depth per stream.",
	}, []string{"stream"})

	RateLimitRejections = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "rate_limit_rejections_total",
		Help:      "Rate limit rejections by type (user or ip).",
	}, []string{"type"})

	PoisonMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "poison_messages_total",
		Help:      "Messages dead-lettered after exceeding max delivery count.",
	})

	ReclaimerClaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "reclaimer_claims_total",
		Help:      "Stuck messages the reclaimer took over, by stream and result (recovered: reprocessed and acked; poison: dead-lettered).",
	}, []string{"stream", "result"})

	CompileCacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "compile_cache_hits_total",
		Help:      "Compile cache hits — source was found in cache, skipping recompilation.",
	})

	CompileCacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ac",
		Name:      "compile_cache_misses_total",
		Help:      "Compile cache misses — source was not in cache, compilation required.",
	})

	QueueWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "queue_wait_seconds",
		Help:      "Time a job waited in its stream between being enqueued and being read by a worker.",
		Buckets:   []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"stream"})

	// JobStageSeconds splits a job's time into its stages so a slow p99 can be
	// attributed: claim, load_tests, run, persist and publish. Compile has its
	// own histogram (CompileDuration) because it is labelled by language.
	JobStageSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "job_stage_seconds",
		Help:      "Time spent in each stage of processing a job.",
		Buckets:   []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"stage"})
)
