package config

import (
	"testing"
	"time"
)

func validConfig() Config {
	// Hermetic test fixture: the values below are dummies, not real credentials.
	return Config{ //nolint:gosec // G101: dummy, non-secret values in a unit-test fixture
		GatewayAddr:            "127.0.0.1:5100",
		DatabaseURL:            "postgres://aethercode:aethercode@127.0.0.1:5433/aethercode_exec",
		RedisAddr:              "127.0.0.1:6379",
		JudgeURL:               "http://127.0.0.1:5050",
		JudgeToken:             "a-secret-token",
		WorkerCount:            8,
		StreamName:             "ac:submissions",
		ConsumerGroup:          "workers",
		AdmissionMaxQueueDepth: 5000,
		RateLimitPerMinute:     30,
		RateLimitBurst:         10,
		SubmitWeight:           3,
		JobTimeout:             90 * time.Second,
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config should pass validation: %v", err)
	}
}

func TestValidateRejectsDevToken(t *testing.T) {
	c := validConfig()
	c.JudgeToken = "stj-spike-2024"
	if err := c.Validate(); err == nil {
		t.Fatal("the development default token must be rejected")
	}
}

func TestValidateRequiresFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"worker count zero", func(c *Config) { c.WorkerCount = 0 }},
		{"worker count too high", func(c *Config) { c.WorkerCount = 513 }},
		{"empty gateway addr", func(c *Config) { c.GatewayAddr = "" }},
		{"empty database url", func(c *Config) { c.DatabaseURL = "" }},
		{"empty judge url", func(c *Config) { c.JudgeURL = "" }},
		{"empty judge token", func(c *Config) { c.JudgeToken = "" }},
		{"zero job timeout", func(c *Config) { c.JobTimeout = 0 }},
		{"zero submit weight", func(c *Config) { c.SubmitWeight = 0 }},
		{"auth enabled without audience", func(c *Config) { c.AuthEnabled = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected validation error for %q", tc.name)
			}
		})
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("AC_TEST_INT", "42")
	if got := envInt("AC_TEST_INT", 1); got != 42 {
		t.Fatalf("envInt set = %d, want 42", got)
	}
	if got := envInt("AC_TEST_UNSET", 7); got != 7 {
		t.Fatalf("envInt unset = %d, want fallback 7", got)
	}
	t.Setenv("AC_TEST_BAD", "not-a-number")
	if got := envInt("AC_TEST_BAD", 9); got != 9 {
		t.Fatalf("envInt invalid = %d, want fallback 9", got)
	}
}

func TestEnvBool(t *testing.T) {
	t.Setenv("AC_TEST_BOOL", "true")
	if got := envBool("AC_TEST_BOOL", false); got != true {
		t.Fatalf("envBool = %v, want true", got)
	}
	t.Setenv("AC_TEST_BOOL_BAD", "maybe")
	if got := envBool("AC_TEST_BOOL_BAD", true); got != true {
		t.Fatalf("envBool invalid = %v, want fallback true", got)
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("AC_TEST_DUR", "5s")
	if got := envDuration("AC_TEST_DUR", time.Second); got != 5*time.Second {
		t.Fatalf("envDuration = %v, want 5s", got)
	}
	if got := envDuration("AC_TEST_DUR_UNSET", 3*time.Second); got != 3*time.Second {
		t.Fatalf("envDuration unset = %v, want fallback 3s", got)
	}
}
