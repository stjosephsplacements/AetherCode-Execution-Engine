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
			if got := JudgeOutput("exact", c.expected, c.actual); got != c.want {
				t.Fatalf("JudgeOutput(exact, %q, %q) = %q, want %q", c.expected, c.actual, got, c.want)
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
	if JudgeOutput("exact", "1\n2\n", "1\r\n2\r\n") != model.VerdictAccepted {
		t.Fatal("CRLF output should match LF expected")
	}
	if JudgeOutput("exact", "1\n2", "1\r2") != model.VerdictAccepted {
		t.Fatal("lone CR should count as a line break")
	}
}

func TestJudgeToken(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		actual   string
		want     model.Verdict
	}{
		{"exact same", "1 2 3", "1 2 3", model.VerdictAccepted},
		{"extra whitespace in actual", "1 2 3", "1  2  3", model.VerdictAccepted},
		{"extra newlines", "1\n2\n3\n", "1 2 3", model.VerdictAccepted},
		{"mixed whitespace", "hello\tworld", "hello world", model.VerdictAccepted},
		{"wrong token", "1 2 3", "1 2 4", model.VerdictWrongAnswer},
		{"too few tokens", "1 2 3", "1 2", model.VerdictWrongAnswer},
		{"too many tokens", "1 2", "1 2 3", model.VerdictWrongAnswer},
		{"empty both", "", "", model.VerdictAccepted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := JudgeOutput("token", c.expected, c.actual); got != c.want {
				t.Fatalf("JudgeOutput(token, %q, %q) = %q, want %q", c.expected, c.actual, got, c.want)
			}
		})
	}
}

func TestJudgeFloat(t *testing.T) {
	cases := []struct {
		name     string
		expected string
		actual   string
		want     model.Verdict
	}{
		{"exact float", "3.14", "3.14", model.VerdictAccepted},
		{"within epsilon", "1.0", "1.0000001", model.VerdictAccepted},
		{"outside epsilon", "1.0", "1.01", model.VerdictWrongAnswer},
		{"multiple floats pass", "1.0 2.0 3.0", "1.0 2.0 3.0", model.VerdictAccepted},
		{"multiple floats one fails", "1.0 2.0", "1.0 3.0", model.VerdictWrongAnswer},
		{"non-float exact match", "YES", "YES", model.VerdictAccepted},
		{"non-float mismatch", "YES", "NO", model.VerdictWrongAnswer},
		{"token count mismatch", "1.0 2.0", "1.0", model.VerdictWrongAnswer},
		{"negative numbers", "-1.5", "-1.5", model.VerdictAccepted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := JudgeOutput("float", c.expected, c.actual); got != c.want {
				t.Fatalf("JudgeOutput(float, %q, %q) = %q, want %q", c.expected, c.actual, got, c.want)
			}
		})
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

func BenchmarkEqualNormalized_ZeroAlloc(b *testing.B) {
	var sb strings.Builder
	for range 50 {
		sb.WriteString("Hello World 42  \n")
	}
	expected := sb.String()
	actual := sb.String()
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if !equalNormalized(expected, actual) {
			b.Fatal("expected match")
		}
	}
}

func BenchmarkJudgeToken(b *testing.B) {
	var sb strings.Builder
	for range 100 {
		sb.WriteString("12345 67890 hello world 3.14159\n")
	}
	expected := sb.String()
	actual := sb.String()
	b.ResetTimer()
	for range b.N {
		JudgeOutput("token", expected, actual)
	}
}

func BenchmarkJudgeFloat(b *testing.B) {
	var sb strings.Builder
	for range 100 {
		sb.WriteString("3.14159265 2.71828182 1.41421356 1.7320508\n")
	}
	expected := sb.String()
	actual := sb.String()
	b.ResetTimer()
	for range b.N {
		JudgeOutput("float", expected, actual)
	}
}
