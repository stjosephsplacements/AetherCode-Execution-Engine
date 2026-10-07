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
