package sandbox

import (
	"slices"
	"testing"
)

func TestEffectiveDefaultsWithoutLimits(t *testing.T) {
	lang := Languages["cpp"]
	cpu, mem := lang.Effective(RunLimits{})
	if cpu != lang.RunCPU || mem != lang.RunMem {
		t.Fatalf("got cpu=%d mem=%d, want language defaults %d %d", cpu, mem, lang.RunCPU, lang.RunMem)
	}
}

func TestEffectiveAppliesRequestLimits(t *testing.T) {
	cpu, mem := Languages["cpp"].Effective(RunLimits{CPU: 1_000_000_000, Memory: 64 << 20})
	if cpu != 1_000_000_000 || mem != 64<<20 {
		t.Fatalf("got cpu=%d mem=%d", cpu, mem)
	}
}

func TestEffectiveKeepsJVMMemoryFloor(t *testing.T) {
	_, mem := Languages["java"].Effective(RunLimits{Memory: 32 << 20})
	if mem != Languages["java"].MinRunMem {
		t.Fatalf("java memory %d, want floor %d", mem, Languages["java"].MinRunMem)
	}
}

func TestBuildRunCmdSetsLimitsAndClock(t *testing.T) {
	cmd := BuildRunCmd(Languages["python"], "print(1)", "", "", RunLimits{CPU: 3_000_000_000, Memory: 128 << 20})
	if cmd.CPULimit != 3_000_000_000 || cmd.MemoryLimit != 128<<20 || cmd.ClockLimit != 7_000_000_000 {
		t.Fatalf("cpu=%d mem=%d clock=%d", cmd.CPULimit, cmd.MemoryLimit, cmd.ClockLimit)
	}
}

func TestBuildCompileCmdRustDefaults(t *testing.T) {
	rustc := []string{"/usr/bin/rustc", "-O", "-o", "/w/a.out", "/w/main.rs"}
	tests := []struct {
		name  string
		flags []string
		want  []string
	}{
		{"no flags compile as edition 2021", nil, append(slices.Clone(rustc), "--edition=2021")},
		{"caller edition replaces the default", []string{"--edition=2018"}, append(slices.Clone(rustc), "--edition=2018")},
		{"caller 2021 is not repeated", []string{"--edition=2021"}, append(slices.Clone(rustc), "--edition=2021")},
		// rustc accepts -O twice, so a caller -O keeps the default edition.
		{"other flags keep the default", []string{"-O"}, append(slices.Clone(rustc), "--edition=2021", "-O")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildCompileCmdWithOptions(Languages["rust"], "fn main(){}", nil, tt.flags).Args
			if !slices.Equal(got, tt.want) {
				t.Fatalf("args = %q, want %q", got, tt.want)
			}
		})
	}
}

// A language without CompileDefaults gets its args and the caller's flags, as before.
func TestBuildCompileCmdWithoutDefaults(t *testing.T) {
	cpp := Languages["cpp"]
	got := BuildCompileCmdWithOptions(cpp, "int main(){}", nil, []string{"-O3"}).Args
	want := append(slices.Clone(cpp.CompileArgs), "-O3")
	if !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

// No allowed caller flag may repeat a default: rustc fails with "Option
// 'edition' given more than once" when --edition appears twice.
func TestBuildCompileCmdNeverRepeatsAllowedFlag(t *testing.T) {
	for language, allowed := range AllowedCompilerFlags {
		lang, ok := Languages[language]
		if !ok || len(lang.CompileDefaults) == 0 {
			continue
		}
		for flag := range allowed {
			args := BuildCompileCmdWithOptions(lang, "", nil, []string{flag}).Args
			seen := map[string]bool{}
			for _, arg := range args[len(lang.CompileArgs):] {
				if seen[flagName(arg)] {
					t.Errorf("%s with %q: %q repeats %s", language, flag, args, flagName(arg))
				}
				seen[flagName(arg)] = true
			}
		}
	}
}
