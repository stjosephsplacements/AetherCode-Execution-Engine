package sandbox

// StdoutMax and StderrMax are the per-test output caps sent to go-judge.
// They default to 8 MiB and 1 MiB respectively and can be overridden at
// startup via AC_SANDBOX_STDOUT_MAX_BYTES / AC_SANDBOX_STDERR_MAX_BYTES.
var (
	StdoutMax int64 = 8 << 20
	StderrMax int64 = 1 << 20
)

const (
	// Compiler diagnostics stay small — not configurable.
	compileOutMax int64 = 64 << 10
)

// Bounds for per-request run limits (RunLimits); requests outside them are rejected.
const (
	MinRunCPU = 100_000_000    // 100ms
	MaxRunCPU = 20_000_000_000 // 20s
	MinRunMem = 16 << 20       // 16MB
	MaxRunMem = 2 << 30        // 2GB
)

// RunLimits overrides a language's default CPU time (ns) and memory (bytes)
// limits for one job. Zero fields keep the language default.
type RunLimits struct {
	CPU    uint64
	Memory uint64
}

// Effective returns the CPU and memory limits a run uses. Memory never drops
// below the language's MinRunMem: a JVM cannot start in a small question limit.
func (lang LangConfig) Effective(limits RunLimits) (cpu, memory uint64) {
	cpu, memory = lang.RunCPU, lang.RunMem
	if limits.CPU != 0 {
		cpu = limits.CPU
	}
	if limits.Memory != 0 {
		memory = max(limits.Memory, lang.MinRunMem)
	}
	return cpu, memory
}

// ClockLimit is the wall-clock limit for a run: twice the CPU limit plus a
// second, so a program blocked on sleep or I/O cannot hold a worker forever.
func ClockLimit(cpu uint64) uint64 {
	return 2*cpu + 1_000_000_000
}

type LangConfig struct {
	Compiled    bool
	CompileArgs []string
	CompileEnv  []string
	CompileCPU  uint64 // nanoseconds
	CompileMem  uint64 // bytes
	CompileProc uint64
	SourcePath  string
	BinaryPath  string // output path for compiled languages

	RunArgs []string
	RunEnv  []string
	RunCPU  uint64
	RunMem  uint64
	RunProc uint64
	// MinRunMem is the floor for a per-request memory limit (JVM heap + metaspace).
	MinRunMem uint64

	// StdinIsSource means the source is delivered via stdin (files[0])
	// instead of via copyIn. Used by SQLite.
	StdinIsSource bool
}

var Languages = map[string]LangConfig{
	"c": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/gcc", "-o", "/w/a.out", "/w/main.c", "-O2", "-pipe", "-lm"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin"},
		CompileCPU:  10_000_000_000,
		CompileMem:  268_435_456,
		CompileProc: 50,
		SourcePath:  "/w/main.c",
		BinaryPath:  "/w/a.out",
		RunArgs:     []string{"/w/a.out"},
		RunEnv:      []string{"PATH=/usr/bin:/bin"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     10,
	},
	"cpp": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/g++", "-o", "/w/a.out", "/w/main.cpp", "-O2", "-pipe", "-std=c++17"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin"},
		CompileCPU:  10_000_000_000,
		CompileMem:  268_435_456,
		CompileProc: 50,
		SourcePath:  "/w/main.cpp",
		BinaryPath:  "/w/a.out",
		RunArgs:     []string{"/w/a.out"},
		RunEnv:      []string{"PATH=/usr/bin:/bin"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     10,
	},
	"go": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/go", "build", "-o", "/w/main", "/w/main.go"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin", "GOROOT=/usr/lib/go-1.22", "HOME=/tmp", "GOCACHE=/tmp/go-cache"},
		CompileCPU:  30_000_000_000,
		CompileMem:  1_073_741_824,
		CompileProc: 512,
		SourcePath:  "/w/main.go",
		BinaryPath:  "/w/main",
		RunArgs:     []string{"/w/main"},
		RunEnv:      []string{"PATH=/usr/bin:/bin"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     20,
	},
	"java": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/javac", "-d", "/w", "/w/Main.java"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin", "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"},
		CompileCPU:  15_000_000_000,
		CompileMem:  536_870_912,
		CompileProc: 50,
		SourcePath:  "/w/Main.java",
		BinaryPath:  "/w/Main.class",
		RunArgs:     []string{"/usr/bin/java", "-XX:+TieredCompilation", "-XX:TieredStopAtLevel=1", "-cp", "/w", "Main"},
		RunEnv:      []string{"PATH=/usr/bin:/bin", "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     50,
		MinRunMem:   268_435_456,
	},
	"python": {
		Compiled:   false,
		SourcePath: "/w/main.py",
		RunArgs:    []string{"/usr/bin/python3", "/w/main.py"},
		RunEnv:     []string{"PATH=/usr/bin:/bin"},
		RunCPU:     2_000_000_000,
		RunMem:     536_870_912,
		RunProc:    50,
	},
	"javascript": {
		Compiled:   false,
		SourcePath: "/w/main.js",
		RunArgs:    []string{"/usr/bin/node", "/w/main.js"},
		RunEnv:     []string{"PATH=/usr/bin:/bin", "HOME=/tmp"},
		RunCPU:     2_000_000_000,
		RunMem:     536_870_912,
		RunProc:    50,
	},
	"sqlite": {
		Compiled:      false,
		StdinIsSource: true,
		RunArgs:       []string{"/usr/bin/sqlite3", ":memory:"},
		RunEnv:        []string{"PATH=/usr/bin:/bin"},
		RunCPU:        2_000_000_000,
		RunMem:        536_870_912,
		RunProc:       10,
	},
	// --- Additional languages ---
	"bash": {
		Compiled:   false,
		SourcePath: "/w/main.sh",
		RunArgs:    []string{"/bin/bash", "/w/main.sh"},
		RunEnv:     []string{"PATH=/usr/bin:/bin"},
		RunCPU:     2_000_000_000,
		RunMem:     268_435_456,
		RunProc:    20,
	},
	"csharp": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/mcs", "-out:/w/main.exe", "/w/Main.cs"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin"},
		CompileCPU:  15_000_000_000,
		CompileMem:  536_870_912,
		CompileProc: 50,
		SourcePath:  "/w/Main.cs",
		BinaryPath:  "/w/main.exe",
		RunArgs:     []string{"/usr/bin/mono", "/w/main.exe"},
		RunEnv:      []string{"PATH=/usr/bin:/bin"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     20,
		MinRunMem:   134_217_728,
	},
	"kotlin": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/kotlinc", "/w/Main.kt", "-include-runtime", "-d", "/w/Main.jar"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin", "KOTLIN_HOME=/usr/share/kotlin"},
		CompileCPU:  20_000_000_000,
		CompileMem:  1_073_741_824,
		CompileProc: 50,
		SourcePath:  "/w/Main.kt",
		BinaryPath:  "/w/Main.jar",
		RunArgs:     []string{"/usr/bin/java", "-jar", "/w/Main.jar"},
		RunEnv:      []string{"PATH=/usr/bin:/bin", "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     50,
		MinRunMem:   268_435_456,
	},
	"rust": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/rustc", "-O", "-o", "/w/a.out", "/w/main.rs"},
		CompileEnv:  []string{"PATH=/usr/bin:/bin", "HOME=/tmp"},
		CompileCPU:  20_000_000_000,
		CompileMem:  536_870_912,
		CompileProc: 50,
		SourcePath:  "/w/main.rs",
		BinaryPath:  "/w/a.out",
		RunArgs:     []string{"/w/a.out"},
		RunEnv:      []string{"PATH=/usr/bin:/bin"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     10,
	},
	"typescript": {
		Compiled:   false,
		SourcePath: "/w/main.ts",
		RunArgs:    []string{"/usr/bin/bun", "run", "/w/main.ts"},
		RunEnv:     []string{"PATH=/usr/bin:/bin", "HOME=/tmp"},
		RunCPU:     2_000_000_000,
		RunMem:     536_870_912,
		RunProc:    50,
	},
	"php": {
		Compiled:   false,
		SourcePath: "/w/main.php",
		RunArgs:    []string{"/usr/bin/php", "/w/main.php"},
		RunEnv:     []string{"PATH=/usr/bin:/bin", "HOME=/tmp"},
		RunCPU:     2_000_000_000,
		RunMem:     536_870_912,
		RunProc:    50,
	},
	"ruby": {
		Compiled:   false,
		SourcePath: "/w/main.rb",
		RunArgs:    []string{"/usr/bin/ruby", "/w/main.rb"},
		RunEnv:     []string{"PATH=/usr/bin:/bin", "HOME=/tmp"},
		RunCPU:     2_000_000_000,
		RunMem:     536_870_912,
		RunProc:    50,
	},
}

// BuildCompileCmd builds the go-judge Cmd for the compilation step.
func BuildCompileCmd(lang LangConfig, sourceCode string) Cmd {
	return BuildCompileCmdWithOptions(lang, sourceCode, nil, nil)
}

// BuildCompileCmdWithOptions builds the go-judge Cmd for compilation supporting extra files and compiler flags.
func BuildCompileCmdWithOptions(lang LangConfig, sourceCode string, extraFiles map[string]string, compilerFlags []string) Cmd {
	args := make([]string, 0, len(lang.CompileArgs)+len(compilerFlags))
	args = append(args, lang.CompileArgs...)
	if len(compilerFlags) > 0 {
		args = append(args, compilerFlags...)
	}

	copyIn := make(map[string]CopyIn, 1+len(extraFiles))
	copyIn[lang.SourcePath] = FileContent(sourceCode)
	for fname, content := range extraFiles {
		// Place extra files relative to /w/
		targetPath := "/w/" + fname
		copyIn[targetPath] = FileContent(content)
	}

	return Cmd{
		Args:          args,
		Env:           lang.CompileEnv,
		Files:         []CmdFile{Stdin(""), Collector("stdout", compileOutMax), Collector("stderr", compileOutMax)},
		CPULimit:      lang.CompileCPU,
		ClockLimit:    ClockLimit(lang.CompileCPU),
		MemoryLimit:   lang.CompileMem,
		ProcLimit:     lang.CompileProc,
		CopyIn:        copyIn,
		CopyOut:       []string{"stdout", "stderr"},
		CopyOutCached: []string{lang.BinaryPath},
	}
}

// BuildCacheSourceCmd builds a no-op go-judge Cmd that uploads interpreted source
// into the file cache. The returned fileID can then be used in BuildRunCmd to avoid
// sending the full source text in every test Cmd of a batch.
func BuildCacheSourceCmd(lang LangConfig, sourceCode string) Cmd {
	return Cmd{
		Args:          []string{"/bin/true"},
		Env:           []string{},
		Files:         []CmdFile{Stdin(""), Collector("stdout", 0), Collector("stderr", 0)},
		CPULimit:      1_000_000_000,
		ClockLimit:    2_000_000_000,
		MemoryLimit:   4 << 20,
		ProcLimit:     1,
		CopyIn:        map[string]CopyIn{lang.SourcePath: FileContent(sourceCode)},
		CopyOutCached: []string{lang.SourcePath},
	}
}

// BuildRunCmd builds the go-judge Cmd for a single test execution.
// For compiled languages, pass the fileID from the compile step.
// For interpreted languages with a cached source fileID, pass it to avoid
// embedding source in every Cmd; pass empty string to inline the source.
func BuildRunCmd(lang LangConfig, sourceCode, stdinData, fileID string, limits RunLimits) Cmd {
	return BuildRunCmdWithOptions(lang, sourceCode, stdinData, fileID, limits, nil)
}

// BuildRunCmdWithOptions builds the go-judge Cmd supporting extra files for interpreted runs.
func BuildRunCmdWithOptions(lang LangConfig, sourceCode, stdinData, fileID string, limits RunLimits, extraFiles map[string]string) Cmd {
	cpu, memory := lang.Effective(limits)
	cmd := Cmd{
		Args:        lang.RunArgs,
		Env:         lang.RunEnv,
		Files:       []CmdFile{Stdin(stdinData), Collector("stdout", StdoutMax), Collector("stderr", StderrMax)},
		CPULimit:    cpu,
		ClockLimit:  ClockLimit(cpu),
		MemoryLimit: memory,
		ProcLimit:   lang.RunProc,
		CopyOut:     []string{"stdout", "stderr"},
	}

	copyIn := make(map[string]CopyIn)
	if fileID != "" {
		target := lang.BinaryPath
		if !lang.Compiled {
			target = lang.SourcePath
		}
		copyIn[target] = CachedFile(fileID)
	} else if lang.StdinIsSource {
		cmd.Files[0] = Stdin(sourceCode)
	} else if !lang.Compiled {
		copyIn[lang.SourcePath] = FileContent(sourceCode)
	}

	for fname, content := range extraFiles {
		targetPath := "/w/" + fname
		copyIn[targetPath] = FileContent(content)
	}
	if len(copyIn) > 0 {
		cmd.CopyIn = copyIn
	}

	return cmd
}
