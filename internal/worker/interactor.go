package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

// interactor holds runtime configuration for an interactive problem interactor.
type interactor struct {
	language   string
	sourceCode string
	langConfig sandbox.LangConfig
	fileID     string // for compiled interactors or cached interpreted scripts
}

// prepareInteractor compiles (if compiled) or caches (if interpreted) the interactor.
func (w *Worker) prepareInteractor(ctx context.Context, interactorLang, interactorCode string, log *slog.Logger) (*interactor, error) {
	if interactorLang == "" {
		interactorLang = "python"
	}
	langCfg, ok := sandbox.Languages[interactorLang]
	if !ok {
		return nil, fmt.Errorf("unsupported interactor language: %s", interactorLang)
	}

	it := &interactor{
		language:   interactorLang,
		sourceCode: interactorCode,
		langConfig: langCfg,
	}

	cacheKey := compileCacheKey("interactor:"+interactorLang, interactorCode)
	if cachedID, hit := w.cc.get(cacheKey); hit {
		it.fileID = cachedID
		log.Debug("interactor cache hit", "lang", interactorLang, "fileID", cachedID)
		return it, nil
	}

	if langCfg.Compiled {
		compileCmd := sandbox.BuildCompileCmd(langCfg, interactorCode)
		results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{compileCmd}})
		if err != nil {
			return nil, fmt.Errorf("compile interactor: %w", err)
		}
		if len(results) == 0 || results[0].Status != sandbox.StatusAccepted {
			stderr := ""
			if len(results) > 0 {
				stderr = results[0].Files["stderr"]
			}
			return nil, fmt.Errorf("interactor compilation failed: %s", stderr)
		}
		for _, fid := range results[0].FileIDs {
			it.fileID = fid
			break
		}
		if it.fileID != "" {
			w.cc.put(cacheKey, it.fileID)
		}
	} else if !langCfg.StdinIsSource {
		cacheCmd := sandbox.BuildCacheSourceCmd(langCfg, interactorCode)
		results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{cacheCmd}})
		if err == nil && len(results) == 1 && results[0].Status == sandbox.StatusAccepted {
			for _, fid := range results[0].FileIDs {
				it.fileID = fid
				break
			}
			if it.fileID != "" {
				w.cc.put(cacheKey, it.fileID)
			}
		}
	}

	return it, nil
}

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
	it *interactor,
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
