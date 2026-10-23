package worker

import (
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

// buildInteractiveCmds constructs the dual Cmd request and pipe mapping for an interactive testcase.
// Cmd[0]: Student program
// Cmd[1]: Interactor program (given input file and expected answer file)
// Pipes:
//
//	Student stdout (fd 1) -> Interactor stdin (fd 0)
//	Interactor stdout (fd 1) -> Student stdin (fd 0)
func buildInteractiveCmds(
	studentLang sandbox.LangConfig,
	studentSource string,
	studentFileID string,
	studentLimits sandbox.RunLimits,
	extraFiles map[string]string,
	it *sandboxProgram,
	inputData string,
	expectedOutput string,
) ([]sandbox.Cmd, []sandbox.PipeMap) {
	// Build student Cmd: stdin and stdout are pipes (null), stderr is collected
	studentCmd := sandbox.BuildRunCmdWithOptions(studentLang, studentSource, "", studentFileID, studentLimits, extraFiles)
	// Stdin (index 0) and Stdout (index 1) are handled by pipeMapping, so Files must have null placeholders
	studentCmd.Files = []sandbox.CmdFile{
		{}, // fd 0: pipe in from interactor
		{}, // fd 1: pipe out to interactor
		sandbox.Collector("stderr", sandbox.StderrMax),
	}

	// Build interactor Cmd
	interactorCPU := uint64(5_000_000_000)
	interactorClock := sandbox.ClockLimit(interactorCPU)
	interactorMem := uint64(512 << 20)

	interactorArgs := make([]string, 0, len(it.langConfig.RunArgs)+2)
	interactorArgs = append(interactorArgs, it.langConfig.RunArgs...)
	interactorArgs = append(interactorArgs, "/w/input.txt", "/w/expected.txt")

	interactorCopyIn := make(map[string]sandbox.CopyIn)
	interactorCopyIn["/w/input.txt"] = sandbox.FileContent(inputData)
	interactorCopyIn["/w/expected.txt"] = sandbox.FileContent(expectedOutput)

	if it.fileID != "" {
		target := it.langConfig.BinaryPath
		if !it.langConfig.Compiled {
			target = it.langConfig.SourcePath
		}
		interactorCopyIn[target] = sandbox.CachedFile(it.fileID)
	} else if !it.langConfig.Compiled {
		interactorCopyIn[it.langConfig.SourcePath] = sandbox.FileContent(it.sourceCode)
	}

	interactorCmd := sandbox.Cmd{
		Args:        interactorArgs,
		Env:         it.langConfig.RunEnv,
		Files:       []sandbox.CmdFile{{}, {}, sandbox.Collector("stderr", 64<<10)},
		CPULimit:    interactorCPU,
		ClockLimit:  interactorClock,
		MemoryLimit: interactorMem,
		ProcLimit:   it.langConfig.RunProc,
		CopyIn:      interactorCopyIn,
		CopyOut:     []string{"stderr"},
	}

	cmds := []sandbox.Cmd{studentCmd, interactorCmd}

	// Wire bidirectional pipes
	pipeMapping := []sandbox.PipeMap{
		{
			// Student stdout (index 0, fd 1) -> Interactor stdin (index 1, fd 0)
			In:    sandbox.PipeIndex{Index: 0, Fd: 1},
			Out:   sandbox.PipeIndex{Index: 1, Fd: 0},
			Proxy: true,
		},
		{
			// Interactor stdout (index 1, fd 1) -> Student stdin (index 0, fd 0)
			In:    sandbox.PipeIndex{Index: 1, Fd: 1},
			Out:   sandbox.PipeIndex{Index: 0, Fd: 0},
			Proxy: true,
		},
	}

	return cmds, pipeMapping
}
