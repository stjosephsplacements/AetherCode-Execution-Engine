package config

import (
	"encoding/hex"
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
	AuthEnabled    bool
	AllowUnauth    bool // AC_ALLOW_UNAUTH: explicit opt-in to disable auth (must be true when AuthEnabled is false)
	OIDCIssuer     string
	OIDCJWKSURL    string
	OIDCAudience   string
	DatabaseAppURL string // optional: connects as aethercode_app for RLS-enforced auth queries

	// Rate limiting
	RateLimitPerMinute       int
	RateLimitBurst           int
	RateLimitUnauthPerMinute int // per source IP, 0 = disabled
	RateLimitUnauthBurst     int
	AdmissionMaxQueueDepth   int

	// CORS
	CORSAllowedOrigins string

	// Reverse proxy
	TrustedProxies []string // AC_TRUSTED_PROXIES: comma-separated CIDRs

	// Fair queuing
	SubmitStreamName string
	RunStreamName    string
	BulkStreamName   string
	SubmitWeight     int
	BulkWeight       int

	// Timeouts
	JobTimeout time.Duration

	// Worker tuning
	TestParallelism int
	CompileCacheTTL time.Duration

	// Queue tuning
	BlockReadTimeout time.Duration

	// Go runtime tuning
	GOGC       int
	GOMEMLIMIT int64

	// Sandbox output limits
	SandboxStdoutMaxBytes int64
	SandboxStderrMaxBytes int64

	// Webhook security: AES-256-GCM master key used to encrypt callback_secret in Redis.
	// 32 bytes encoded as 64 hex characters.  Optional: if empty, callback_secret is
	// stored without encryption (backwards-compatible, but discouraged).
	WebhookMasterKey []byte
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

		AuthEnabled:    envBool("AC_AUTH_ENABLED", true),
		AllowUnauth:    envBool("AC_ALLOW_UNAUTH", false),
		OIDCIssuer:     env("AC_OIDC_ISSUER", "https://sso.example.com"),
		OIDCJWKSURL:    env("AC_OIDC_JWKS_URL", "http://127.0.0.1:8087/oauth/v2/keys"),
		OIDCAudience:   env("AC_OIDC_AUDIENCE", ""),
		DatabaseAppURL: env("AC_DATABASE_APP_URL", ""),

		RateLimitPerMinute:       envInt("AC_RATE_LIMIT_PER_MINUTE", 30),
		RateLimitBurst:           envInt("AC_RATE_LIMIT_BURST", 10),
		RateLimitUnauthPerMinute: envInt("AC_RATE_LIMIT_UNAUTH_PER_MINUTE", 0),
		RateLimitUnauthBurst:     envInt("AC_RATE_LIMIT_UNAUTH_BURST", 5),
		AdmissionMaxQueueDepth:   envInt("AC_ADMISSION_MAX_QUEUE_DEPTH", 5000),

		CORSAllowedOrigins: env("AC_CORS_ALLOWED_ORIGINS", ""),

		TrustedProxies: envList("AC_TRUSTED_PROXIES"),

		SubmitStreamName: env("AC_SUBMIT_STREAM", "ac:submissions:submit"),
		RunStreamName:    env("AC_RUN_STREAM", "ac:submissions:run"),
		BulkStreamName:   env("AC_BULK_STREAM", "ac:submissions:bulk"),
		SubmitWeight:     envInt("AC_SUBMIT_WEIGHT", 3),
		BulkWeight:       envInt("AC_BULK_WEIGHT", 1),

		JobTimeout: envDuration("AC_JOB_TIMEOUT", 90*time.Second),

		TestParallelism:  envInt("AC_TEST_PARALLELISM", 4),
		CompileCacheTTL:  envDuration("AC_COMPILE_CACHE_TTL", 60*time.Second),
		BlockReadTimeout: envDuration("AC_BLOCK_READ_TIMEOUT", 500*time.Millisecond),

		GOGC:       envInt("AC_GOGC", 200),
		GOMEMLIMIT: envInt64("AC_GOMEMLIMIT", 0),

		SandboxStdoutMaxBytes: envInt64("AC_SANDBOX_STDOUT_MAX_BYTES", 8<<20),
		SandboxStderrMaxBytes: envInt64("AC_SANDBOX_STDERR_MAX_BYTES", 1<<20),

		WebhookMasterKey: envHex("AC_WEBHOOK_MASTER_KEY", 32),
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
	check(c.BulkWeight > 0, "BulkWeight must be > 0")
	check(c.AdmissionMaxQueueDepth > 0, "AdmissionMaxQueueDepth must be > 0")
	check(c.RateLimitPerMinute > 0, "RateLimitPerMinute must be > 0")
	check(c.RateLimitBurst > 0, "RateLimitBurst must be > 0")
	check(c.RateLimitUnauthPerMinute >= 0, "RateLimitUnauthPerMinute must be >= 0")
	check(c.RateLimitUnauthBurst > 0, "RateLimitUnauthBurst must be > 0")
	check(c.GatewayAddr != "", "GatewayAddr is required")
	check(c.DatabaseURL != "", "DatabaseURL is required")
	check(c.RedisAddr != "", "RedisAddr is required")
	check(c.JudgeURL != "", "JudgeURL is required")
	check(c.JudgeToken != "", "JudgeToken (AC_JUDGE_TOKEN) is required")
	check(c.JudgeToken != "stj-spike-2024", "JudgeToken must not use the development default value")
	check(c.JudgeToken != "aethercode-dev-token", "JudgeToken must not use the development default value")
	check(!strings.Contains(c.DatabaseURL, "aethercode:aethercode@"), "DatabaseURL must not use the hardcoded development credentials")
	check(c.JobTimeout > 0, "JobTimeout must be > 0")
	check(c.TestParallelism > 0 && c.TestParallelism <= 64, "TestParallelism must be 1-64")
	check(c.CompileCacheTTL >= 0, "CompileCacheTTL must be >= 0")
	check(c.BlockReadTimeout >= 100*time.Millisecond && c.BlockReadTimeout <= 10*time.Second, "BlockReadTimeout must be 100ms-10s")
	check(c.GOGC >= 50 && c.GOGC <= 1000, "GOGC must be 50-1000")
	check(c.GOMEMLIMIT >= 0, "GOMEMLIMIT must be >= 0")
	check(c.SandboxStdoutMaxBytes > 0 && c.SandboxStdoutMaxBytes <= 64<<20, "SandboxStdoutMaxBytes must be 1-67108864")
	check(c.SandboxStderrMaxBytes > 0 && c.SandboxStderrMaxBytes <= 64<<20, "SandboxStderrMaxBytes must be 1-67108864")

	if c.AuthEnabled && c.OIDCAudience == "" {
		errs = append(errs, "OIDCAudience (AC_OIDC_AUDIENCE) is required when auth is enabled")
	}

	if !c.AuthEnabled && !c.AllowUnauth {
		errs = append(errs, "AC_AUTH_ENABLED=false requires AC_ALLOW_UNAUTH=true to confirm this is intentional")
	}
	if !c.AuthEnabled {
		fmt.Fprintln(os.Stderr, "WARNING: AC_AUTH_ENABLED=false — authentication is disabled")
	}
	if c.AuthEnabled && c.DatabaseAppURL == "" {
		fmt.Fprintln(os.Stderr, "WARNING: AC_DATABASE_APP_URL not set — auth queries run as DB owner, bypassing Row-Level Security")
	}
	if len(c.WebhookMasterKey) == 0 {
		fmt.Fprintln(os.Stderr, "WARNING: AC_WEBHOOK_MASTER_KEY not set — callback_secret will be stored in plaintext in Redis; set a 32-byte hex key to enable AES-256-GCM encryption")
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

func envList(key string) []string {
	if v := os.Getenv(key); v != "" {
		var out []string
		for _, s := range strings.Split(v, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
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

// envHex decodes a hex-encoded byte slice from the environment variable named key.
// If the variable is absent or empty, nil is returned (caller treats nil as "not set").
// If the value is present but cannot be decoded, or its decoded length does not equal
// wantBytes, a warning is printed to stderr and nil is returned.
func envHex(key string, wantBytes int) []byte {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	b, err := hex.DecodeString(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %s is not valid hex: %v — ignoring\n", key, err)
		return nil
	}
	if len(b) != wantBytes {
		fmt.Fprintf(os.Stderr, "config: %s must decode to exactly %d bytes (got %d) — ignoring\n", key, wantBytes, len(b))
		return nil
	}
	return b
}
