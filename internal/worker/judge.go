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
	if equalNormalized(expected, actual) {
		return model.VerdictAccepted
	}
	return model.VerdictWrongAnswer
}

// equalNormalized compares expected and actual outputs line-by-line in a single pass
// without allocating strings for intermediate normalized outputs.
func equalNormalized(expected, actual string) bool {
	return normalizeOutput(expected) == normalizeOutput(actual)
}

// nextToken scans string s starting at offset and returns the start and end indices of the next whitespace-delimited token.
// If no tokens remain, start and end are -1.
func nextToken(s string, offset int) (start, end int) {
	n := len(s)
	i := offset
	for i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	if i >= n {
		return -1, -1
	}
	start = i
	for i < n && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' {
		i++
	}
	return start, i
}

// judgeToken splits on whitespace and compares token by token without allocating slices.
func judgeToken(expected, actual string) model.Verdict {
	expOff, actOff := 0, 0
	for {
		eStart, eEnd := nextToken(expected, expOff)
		aStart, aEnd := nextToken(actual, actOff)

		if eStart == -1 && aStart == -1 {
			return model.VerdictAccepted
		}
		if eStart == -1 || aStart == -1 {
			return model.VerdictWrongAnswer
		}

		if expected[eStart:eEnd] != actual[aStart:aEnd] {
			return model.VerdictWrongAnswer
		}

		expOff = eEnd
		actOff = aEnd
	}
}

// judgeFloat compares tokens as float64 with the given epsilon without slice allocations.
func judgeFloat(expected, actual string, epsilon float64) model.Verdict {
	expOff, actOff := 0, 0
	for {
		eStart, eEnd := nextToken(expected, expOff)
		aStart, aEnd := nextToken(actual, actOff)

		if eStart == -1 && aStart == -1 {
			return model.VerdictAccepted
		}
		if eStart == -1 || aStart == -1 {
			return model.VerdictWrongAnswer
		}

		eTok := expected[eStart:eEnd]
		aTok := actual[aStart:aEnd]

		e, err1 := strconv.ParseFloat(eTok, 64)
		a, err2 := strconv.ParseFloat(aTok, 64)
		if err1 != nil || err2 != nil {
			if eTok != aTok {
				return model.VerdictWrongAnswer
			}
		} else {
			if math.IsNaN(e) || math.IsNaN(a) {
				return model.VerdictWrongAnswer
			}
			if math.Abs(e-a) > epsilon {
				return model.VerdictWrongAnswer
			}
		}

		expOff = eEnd
		actOff = aEnd
	}
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
