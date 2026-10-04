package sandbox

const (
	stdoutMax int64 = 65536
	stderrMax int64 = 65536
)

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

	// StdinIsSource means the source is delivered via stdin (files[0])
	// instead of via copyIn. Used by SQLite.
	StdinIsSource bool
}

var Languages = map[string]LangConfig{
	"c": {
		Compiled:    true,
		CompileArgs: []string{"/usr/bin/gcc", "-o", "/w/a.out", "/w/main.c", "-O2", "-lm"},
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
		CompileArgs: []string{"/usr/bin/g++", "-o", "/w/a.out", "/w/main.cpp", "-O2", "-std=c++17"},
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
		RunArgs:     []string{"/usr/bin/java", "-cp", "/w", "Main"},
		RunEnv:      []string{"PATH=/usr/bin:/bin", "JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"},
		RunCPU:      2_000_000_000,
		RunMem:      536_870_912,
		RunProc:     50,
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
}

// BuildCompileCmd builds the go-judge Cmd for the compilation step.
func BuildCompileCmd(lang LangConfig, sourceCode string) Cmd {
	return Cmd{
		Args:  lang.CompileArgs,
		Env:   lang.CompileEnv,
		Files: []CmdFile{Stdin(""), Collector("stdout", stdoutMax), Collector("stderr", stderrMax)},
		CPULimit:      lang.CompileCPU,
		MemoryLimit:   lang.CompileMem,
		ProcLimit:     lang.CompileProc,
		CopyIn:        map[string]CopyIn{lang.SourcePath: FileContent(sourceCode)},
		CopyOut:       []string{"stdout", "stderr"},
		CopyOutCached: []string{lang.BinaryPath},
	}
}

// BuildRunCmd builds the go-judge Cmd for a single test execution.
// For compiled languages, pass the fileID from the compile step.
// For interpreted languages, pass the source code (fileID will be empty).
func BuildRunCmd(lang LangConfig, sourceCode, stdinData, fileID string) Cmd {
	cmd := Cmd{
		Args:        lang.RunArgs,
		Env:         lang.RunEnv,
		Files:       []CmdFile{Stdin(stdinData), Collector("stdout", stdoutMax), Collector("stderr", stderrMax)},
		CPULimit:    lang.RunCPU,
		MemoryLimit: lang.RunMem,
		ProcLimit:   lang.RunProc,
		CopyOut:     []string{"stdout", "stderr"},
	}

	if lang.Compiled && fileID != "" {
		cmd.CopyIn = map[string]CopyIn{lang.BinaryPath: CachedFile(fileID)}
	} else if lang.StdinIsSource {
		// SQLite: source SQL goes through stdin; stdinData (test input) is not applicable.
		cmd.Files[0] = Stdin(sourceCode)
	} else if !lang.Compiled {
		cmd.CopyIn = map[string]CopyIn{lang.SourcePath: FileContent(sourceCode)}
	}

	return cmd
}
