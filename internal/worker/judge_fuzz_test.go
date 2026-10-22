package worker

import (
	"testing"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

func FuzzNormalizeOutput(f *testing.F) {
	// Seed corpus covering:
	// - empty string
	// - string with only spaces
	// - CRLF line endings
	// - trailing newlines
	// - valid UTF-8 multi-byte sequences
	seeds := []string{
		"",
		"   ",
		"\t  \t",
		"hello\r\nworld\r\n",
		"line1\nline2\n\n",
		"trailing spaces   \n",
		"unicode: こんにちは世界 🚀 \n",
		"\r\r\n\n\r",
		"mixed \r\n line \n endings \r here   \n\n",
		"1\n2\n3\n",
		"   leading and trailing   \n   spaces   ",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		// normalizeOutput should never panic or crash, even on invalid UTF-8 bytes.
		norm := normalizeOutput(s)

		// Calling normalizeOutput on already normalized output must be idempotent.
		normAgain := normalizeOutput(norm)
		if norm != normAgain {
			t.Fatalf("normalizeOutput is not idempotent:\ninitial input: %q\nfirst norm: %q\nsecond norm: %q", s, norm, normAgain)
		}
	})
}

func FuzzJudgeOutput(f *testing.F) {
	// Seed corpus covering:
	// - identical inputs
	// - differing only in trailing whitespace
	// - completely different strings
	seeds := []struct {
		checker  string
		expected string
		actual   string
	}{
		{"exact", "", ""},
		{"exact", "hello", "hello"},
		{"exact", "hello\n", "hello\r\n"},
		{"exact", "hello  \n", "hello\n"},
		{"exact", "hello", "world"},
		{"token", "1 2 3", "1   2\n3"},
		{"token", "1 2 3", "1 2 4"},
		{"token", "a b c", "a b"},
		{"float", "3.14159", "3.141592"},
		{"float", "3.14", "not-a-number"},
		{"float", "NaN", "NaN"},
		{"invalid_checker", "abc", "abc"},
	}

	for _, seed := range seeds {
		f.Add(seed.checker, seed.expected, seed.actual)
	}

	validVerdicts := map[model.Verdict]bool{
		model.VerdictAccepted:         true,
		model.VerdictWrongAnswer:      true,
		model.VerdictTimeLimitEx:      true,
		model.VerdictMemoryLimitEx:    true,
		model.VerdictOutputLimitEx:    true,
		model.VerdictCompilationError: true,
		model.VerdictRuntimeError:     true,
		model.VerdictInternalError:    true,
		model.VerdictCancelled:        true,
	}

	f.Fuzz(func(t *testing.T, checker, expected, actual string) {
		verdict := JudgeOutput(checker, expected, actual)
		if !validVerdicts[verdict] {
			t.Fatalf("JudgeOutput(%q, %q, %q) returned unknown verdict: %q", checker, expected, actual, verdict)
		}
	})
}
