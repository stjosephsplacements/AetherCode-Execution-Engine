package sandbox

import "testing"

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
