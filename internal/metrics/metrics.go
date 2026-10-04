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

	JobProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ac",
		Name:      "job_processing_duration_seconds",
		Help:      "Time from dequeue to verdict in seconds.",
		Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 90},
	})

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
)
