package sandbox

import (
	"fmt"
	"strings"
)

// Caller-supplied command-line argument limits. The arguments are passed to
// the program as separate argv entries, never through a shell.
const (
	MaxArgs         = 16
	MaxArgBytes     = 1 << 10
	MaxArgsTotalLen = 4 << 10
)

// ValidateArgs checks the command-line arguments of a run request.
func ValidateArgs(args []string) error {
	if len(args) > MaxArgs {
		return fmt.Errorf("at most %d arguments", MaxArgs)
	}
	total := 0
	for i, arg := range args {
		if len(arg) > MaxArgBytes {
			return fmt.Errorf("argument %d is longer than %d bytes", i, MaxArgBytes)
		}
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("argument %d contains a NUL byte", i)
		}
		total += len(arg)
	}
	if total > MaxArgsTotalLen {
		return fmt.Errorf("arguments total more than %d bytes", MaxArgsTotalLen)
	}
	return nil
}

// runArgs is the program's own argv followed by the caller's arguments. It
// always returns a fresh slice, so the shared language config is never mutated.
func runArgs(base, extra []string) []string {
	out := make([]string, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}
