package worker

import (
	"math"
	"strconv"
	"strings"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

// JudgeOutput compares expected and actual output using the specified checker type.
// Supported types: "exact" (default), "token", "float".
func JudgeOutput(checkerType, expected, actual string) model.Verdict {
	switch checkerType {
	case "token":
		return judgeToken(expected, actual)
	case "float":
		return judgeFloat(expected, actual, 1e-6)
	default: // "exact" or empty
		return judgeExact(expected, actual)
	}
}

func judgeExact(expected, actual string) model.Verdict {
	if normalizeOutput(expected) == normalizeOutput(actual) {
		return model.VerdictAccepted
	}
	return model.VerdictWrongAnswer
}

// judgeToken splits on whitespace and compares token by token.
// Whitespace differences (extra spaces, newlines) are ignored.
func judgeToken(expected, actual string) model.Verdict {
	expTokens := strings.Fields(expected)
	actTokens := strings.Fields(actual)
	if len(expTokens) != len(actTokens) {
		return model.VerdictWrongAnswer
	}
	for i := range expTokens {
		if expTokens[i] != actTokens[i] {
			return model.VerdictWrongAnswer
		}
	}
	return model.VerdictAccepted
}

// judgeFloat compares tokens as float64 with the given epsilon.
// Non-numeric tokens fall back to exact string comparison.
func judgeFloat(expected, actual string, epsilon float64) model.Verdict {
	expTokens := strings.Fields(expected)
	actTokens := strings.Fields(actual)
	if len(expTokens) != len(actTokens) {
		return model.VerdictWrongAnswer
	}
	for i := range expTokens {
		e, err1 := strconv.ParseFloat(expTokens[i], 64)
		a, err2 := strconv.ParseFloat(actTokens[i], 64)
		if err1 != nil || err2 != nil {
			if expTokens[i] != actTokens[i] {
				return model.VerdictWrongAnswer
			}
			continue
		}
		if math.IsNaN(e) || math.IsNaN(a) {
			return model.VerdictWrongAnswer
		}
		if math.Abs(e-a) > epsilon {
			return model.VerdictWrongAnswer
		}
	}
	return model.VerdictAccepted
}

func normalizeOutput(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	lineStart := 0
	trailingWS := -1 // index of first trailing whitespace char in current line
	pendingNL := 0   // empty lines buffered; flushed only when non-empty content follows
	wroteAny := false

	flush := func(line string) {
		if wroteAny {
			for range pendingNL {
				b.WriteByte('\n')
			}
		}
		pendingNL = 0
		if wroteAny {
			b.WriteByte('\n')
		}
		b.WriteString(line)
		wroteAny = true
	}

	emitLine := func(end int) {
		if trailingWS >= 0 && trailingWS < end {
			end = trailingWS
		}
		line := s[lineStart:end]
		if len(line) == 0 {
			if wroteAny {
				pendingNL++
			}
		} else {
			flush(line)
		}
		trailingWS = -1
	}

	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\r':
			emitLine(i)
			if i+1 < len(s) && s[i+1] == '\n' {
				i++
			}
			lineStart = i + 1
		case '\n':
			emitLine(i)
			lineStart = i + 1
		case ' ', '\t':
			if trailingWS < 0 {
				trailingWS = i
			}
		default:
			trailingWS = -1
		}
	}

	// Final line (no trailing newline in input)
	end := len(s)
	if trailingWS >= 0 && trailingWS < end {
		end = trailingWS
	}
	if last := s[lineStart:end]; len(last) > 0 {
		flush(last)
	}
	return b.String()
}
