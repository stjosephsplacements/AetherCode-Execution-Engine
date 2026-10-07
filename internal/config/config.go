package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GatewayAddr   string
	DatabaseURL   string
	RedisAddr     string
	JudgeURL      string
	JudgeToken    string
	WorkerCount   int
	StreamName    string
	ConsumerGroup string
	LogLevel      string
	LogFormat     string

	// Auth (Zitadel OIDC)
	AuthEnabled  bool
	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	// Rate limiting
	RateLimitPerMinute     int
	RateLimitBurst         int
	AdmissionMaxQueueDepth int

	// CORS
	CORSAllowedOrigins string

	// Reverse proxy

	// Fair queuing
	SubmitStreamName string
	RunStreamName    string
	SubmitWeight     int

	// Timeouts
	JobTimeout time.Duration

	// Worker tuning
	TestParallelism int
	CompileCacheTTL time.Duration

	// Queue tuning
	BlockReadTimeout time.Duration

	// Go runtime tuning
	GOGC        int
	GOMEMLIMIT  int64
}

func Load() Config {
	return Config{
		GatewayAddr:   env("AC_GATEWAY_ADDR", "127.0.0.1:5100"),
		DatabaseURL:   env("AC_DATABASE_URL", "postgres://aethercode:aethercode@127.0.0.1:5433/aethercode_exec"),
		RedisAddr:     env("AC_REDIS_ADDR", "127.0.0.1:6379"),
		JudgeURL:      env("AC_JUDGE_URL", "http://127.0.0.1:5050"),
		JudgeToken:    env("AC_JUDGE_TOKEN", ""),
		WorkerCount:   envInt("AC_WORKER_COUNT", 8),
		StreamName:    env("AC_STREAM_NAME", "ac:submissions"),
		ConsumerGroup: env("AC_CONSUMER_GROUP", "workers"),
		LogLevel:      env("AC_LOG_LEVEL", "info"),
		LogFormat:     env("AC_LOG_FORMAT", "json"),

		AuthEnabled:  envBool("AC_AUTH_ENABLED", false),
		OIDCIssuer:   env("AC_OIDC_ISSUER", "https://sso.example.com"),
		OIDCJWKSURL:  env("AC_OIDC_JWKS_URL", "http://127.0.0.1:8087/oauth/v2/keys"),
		OIDCAudience: env("AC_OIDC_AUDIENCE", ""),

		RateLimitPerMinute:     envInt("AC_RATE_LIMIT_PER_MINUTE", 30),
		RateLimitBurst:         envInt("AC_RATE_LIMIT_BURST", 10),
		AdmissionMaxQueueDepth: envInt("AC_ADMISSION_MAX_QUEUE_DEPTH", 5000),

		CORSAllowedOrigins: env("AC_CORS_ALLOWED_ORIGINS", ""),

		SubmitStreamName: env("AC_SUBMIT_STREAM", "ac:submissions:submit"),
		RunStreamName:    env("AC_RUN_STREAM", "ac:submissions:run"),
		SubmitWeight:     envInt("AC_SUBMIT_WEIGHT", 3),

		JobTimeout: envDuration("AC_JOB_TIMEOUT", 90*time.Second),

		TestParallelism:  envInt("AC_TEST_PARALLELISM", 4),
		CompileCacheTTL:  envDuration("AC_COMPILE_CACHE_TTL", 60*time.Second),
		BlockReadTimeout: envDuration("AC_BLOCK_READ_TIMEOUT", 500*time.Millisecond),

		GOGC:       envInt("AC_GOGC", 200),
		GOMEMLIMIT: envInt64("AC_GOMEMLIMIT", 0),
	}
}

func (c Config) Validate() error {
	var errs []string
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, msg)
		}
	}

	check(c.WorkerCount > 0 && c.WorkerCount <= 512, "WorkerCount must be 1-512")
	check(c.SubmitWeight > 0, "SubmitWeight must be > 0")
	check(c.AdmissionMaxQueueDepth > 0, "AdmissionMaxQueueDepth must be > 0")
	check(c.RateLimitPerMinute > 0, "RateLimitPerMinute must be > 0")
	check(c.RateLimitBurst > 0, "RateLimitBurst must be > 0")
	check(c.GatewayAddr != "", "GatewayAddr is required")
	check(c.DatabaseURL != "", "DatabaseURL is required")
	check(c.RedisAddr != "", "RedisAddr is required")
	check(c.JudgeURL != "", "JudgeURL is required")
	check(c.JudgeToken != "", "JudgeToken (AC_JUDGE_TOKEN) is required")
	check(c.JudgeToken != "stj-spike-2024", "JudgeToken must not use the development default value")
	check(c.JobTimeout > 0, "JobTimeout must be > 0")
	check(c.TestParallelism > 0 && c.TestParallelism <= 64, "TestParallelism must be 1-64")
	check(c.CompileCacheTTL >= 0, "CompileCacheTTL must be >= 0")
	check(c.BlockReadTimeout >= 100*time.Millisecond && c.BlockReadTimeout <= 10*time.Second, "BlockReadTimeout must be 100ms-10s")
	check(c.GOGC >= 50 && c.GOGC <= 1000, "GOGC must be 50-1000")
	check(c.GOMEMLIMIT >= 0, "GOMEMLIMIT must be >= 0")

	if c.AuthEnabled && c.OIDCAudience == "" {
		errs = append(errs, "OIDCAudience (AC_OIDC_AUDIENCE) is required when auth is enabled")
	}

	if !c.AuthEnabled {
		fmt.Fprintln(os.Stderr, "WARNING: AC_AUTH_ENABLED=false — authentication is disabled")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: %s=%q is not a valid integer, using default %d\n", key, v, fallback)
			return fallback
		}
		return n
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: %s=%q is not a valid int64, using default %d\n", key, v, fallback)
			return fallback
		}
		return n
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: %s=%q is not a valid boolean, using default %v\n", key, v, fallback)
			return fallback
		}
		return b
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: %s=%q is not a valid duration, using default %v\n", key, v, fallback)
			return fallback
		}
		return d
	}
	return fallback
}
