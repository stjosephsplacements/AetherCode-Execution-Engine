package worker

import (
	"strings"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

// JudgeOutput compares expected and actual output.
// Both are trimmed of trailing whitespace per line and trailing newlines.
func JudgeOutput(expected, actual string) model.Verdict {
	expected = normalizeOutput(expected)
	actual = normalizeOutput(actual)

	if expected == actual {
		return model.VerdictAccepted
	}
	return model.VerdictWrongAnswer
}

func normalizeOutput(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t\r")
	}
	result := strings.Join(lines, "\n")
	return strings.TrimRight(result, "\n")
}
