package worker

import (
	"strings"
	"testing"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

// TestJudgeOutput pins down the output-comparison contract: trailing
// whitespace/newlines are ignored, but any real difference is a wrong answer.
func TestJudgeOutput(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		actual   string
		want     model.Verdict
	}{
		{"exact match", "42\n", "42\n", model.VerdictAccepted},
		{"trailing newline stripped", "42", "42\n", model.VerdictAccepted},
		{"trailing spaces", "42   ", "42", model.VerdictAccepted},
		{"multiple trailing newlines", "42\n\n\n", "42", model.VerdictAccepted},
		{"line-level trailing ws", "a  \nb\t\n", "a\nb\n", model.VerdictAccepted},
		{"crlf line endings", "42\r\n", "42\n", model.VerdictAccepted},
		{"empty vs empty", "", "", model.VerdictAccepted},
		{"different values", "42", "43", model.VerdictWrongAnswer},
		{"empty vs content", "", "x", model.VerdictWrongAnswer},
		{"internal newlines preserved", "a\nb\nc", "a\nb\nd", model.VerdictWrongAnswer},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := JudgeOutput(c.expected, c.actual); got != c.want {
				t.Fatalf("JudgeOutput(%q, %q) = %q, want %q", c.expected, c.actual, got, c.want)
			}
		})
	}
}

// TestNormalizeOutput checks the per-line trimming helper directly.
func TestNormalizeOutput(t *testing.T) {
	cases := map[string]string{
		"a  \n b  \n": "a\n b",
		"line\n\n\n":  "line",
		"pad\t\t":     "pad",
		"a\r\nb\r\n":  "a\nb",
		"no-change":   "no-change",
		"":            "",
	}
	for in, want := range cases {
		if got := normalizeOutput(in); got != want {
			t.Errorf("normalizeOutput(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJudgeOutputNormalizesCarriageReturns(t *testing.T) {
	if JudgeOutput("1\n2\n", "1\r\n2\r\n") != model.VerdictAccepted {
		t.Fatal("CRLF output should match LF expected")
	}
	if JudgeOutput("1\n2", "1\r2") != model.VerdictAccepted {
		t.Fatal("lone CR should count as a line break")
	}
}

func BenchmarkNormalizeOutput(b *testing.B) {
	// Typical output: 50 lines with trailing spaces and newlines
	var sb strings.Builder
	for range 50 {
		sb.WriteString("Hello World 42  \n")
	}
	s := sb.String()
	b.ResetTimer()
	for range b.N {
		normalizeOutput(s)
	}
}
