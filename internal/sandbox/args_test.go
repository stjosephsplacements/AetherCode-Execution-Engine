package sandbox

import (
	"strings"
	"testing"
)

func TestValidateArgs(t *testing.T) {
	t.Parallel()
	tooMany := make([]string, MaxArgs+1)
	cases := map[string]struct {
		args []string
		ok   bool
	}{
		"none":            {nil, true},
		"plain":           {[]string{"-n", "5", "file name"}, true},
		"too many":        {tooMany, false},
		"one too long":    {[]string{strings.Repeat("a", MaxArgBytes+1)}, false},
		"nul byte":        {[]string{"a\x00b"}, false},
		"total too large": {[]string{strings.Repeat("a", MaxArgBytes), strings.Repeat("a", MaxArgBytes), strings.Repeat("a", MaxArgBytes), strings.Repeat("a", MaxArgBytes), "a"}, false},
	}
	for name, tc := range cases {
		if err := ValidateArgs(tc.args); (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok = %v", name, err, tc.ok)
		}
	}
}

func TestRunCmdCarriesArgsStackAndProcs(t *testing.T) {
	t.Parallel()
	lang := Languages["python"]
	before := append([]string(nil), lang.RunArgs...)
	cmd := BuildRunCmd(lang, "print(1)", "", "", RunLimits{Args: []string{"a", "b"}, Stack: 8 << 20, Procs: 4})
	want := append(append([]string(nil), before...), "a", "b")
	if strings.Join(cmd.Args, " ") != strings.Join(want, " ") {
		t.Fatalf("args = %v, want %v", cmd.Args, want)
	}
	if cmd.StackLimit != 8<<20 || cmd.ProcLimit != 4 {
		t.Fatalf("stack = %d, procs = %d, want 8 MiB and 4", cmd.StackLimit, cmd.ProcLimit)
	}
	if strings.Join(Languages["python"].RunArgs, " ") != strings.Join(before, " ") {
		t.Fatal("the language's own run args were changed")
	}
	plain := BuildRunCmd(lang, "print(1)", "", "", RunLimits{})
	if plain.ProcLimit != lang.RunProc || plain.StackLimit != 0 {
		t.Fatalf("defaults changed: procs = %d, stack = %d", plain.ProcLimit, plain.StackLimit)
	}
}

func TestJavaStartsLeanAndKeepsItsExecutables(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"java", "kotlin"} {
		run := Languages[id].RunArgs
		if run[0] != "/usr/bin/java" {
			t.Errorf("%s: run executable = %s, want /usr/bin/java", id, run[0])
		}
		joined := strings.Join(run, " ")
		for _, flag := range []string{"-XX:+UseSerialGC", "-Xshare:auto", "-XX:TieredStopAtLevel=1"} {
			if !strings.Contains(joined, flag) {
				t.Errorf("%s: run args %q lack %s", id, joined, flag)
			}
		}
	}
	// -J flags must come right after javac and before the options it parses.
	if compile := Languages["java"].CompileArgs; compile[0] != "/usr/bin/javac" || !strings.HasPrefix(compile[1], "-J") {
		t.Errorf("javac args = %v, want the -J flags straight after the executable", compile)
	}
}
