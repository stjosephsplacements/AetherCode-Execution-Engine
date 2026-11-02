package sandbox

import (
	"context"
	"fmt"
	"slices"
	"sort"
)

// Binaries lists the executables a language needs inside the sandbox: its
// compiler, if any, and the program that runs it.
func (lang LangConfig) Binaries() []string {
	var out []string
	if lang.Compiled && len(lang.CompileArgs) > 0 {
		out = append(out, lang.CompileArgs[0])
	}
	if len(lang.RunArgs) > 0 && lang.RunArgs[0] != lang.BinaryPath {
		out = append(out, lang.RunArgs[0])
	}
	return out
}

// ProbeLanguages reports, per registered language, whether every executable it
// needs exists in the sandbox. One request checks each distinct executable once.
// An error means the sandbox could not be asked, not that a language is missing.
func (c *Client) ProbeLanguages(ctx context.Context) (map[string]bool, error) {
	seen := map[string]bool{}
	for _, lang := range Languages {
		for _, binary := range lang.Binaries() {
			seen[binary] = true
		}
	}
	binaries := make([]string, 0, len(seen))
	for binary := range seen {
		binaries = append(binaries, binary)
	}
	sort.Strings(binaries)

	cmds := make([]Cmd, len(binaries))
	for i, binary := range binaries {
		cmds[i] = Cmd{
			Args:        []string{"/usr/bin/test", "-x", binary},
			Env:         []string{"PATH=/usr/bin:/bin"},
			Files:       []CmdFile{Stdin(""), Collector("stdout", 0), Collector("stderr", 0)},
			CPULimit:    1_000_000_000,
			ClockLimit:  2_000_000_000,
			MemoryLimit: 16 << 20,
			ProcLimit:   1,
		}
	}
	results, err := c.Run(ctx, Request{Cmd: cmds})
	if err != nil {
		return nil, fmt.Errorf("sandbox: probe languages: %w", err)
	}
	if len(results) != len(binaries) {
		return nil, fmt.Errorf("sandbox: probe languages: got %d results for %d checks", len(results), len(binaries))
	}
	present := make(map[string]bool, len(binaries))
	for i, binary := range binaries {
		present[binary] = results[i].Status == StatusAccepted
	}
	available := make(map[string]bool, len(Languages))
	for id, lang := range Languages {
		available[id] = !slices.ContainsFunc(lang.Binaries(), func(binary string) bool { return !present[binary] })
	}
	return available, nil
}
