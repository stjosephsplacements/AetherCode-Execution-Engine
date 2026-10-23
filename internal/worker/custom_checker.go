package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

// customChecker holds runtime configuration for a custom validator/checker.
type customChecker struct {
	language   string
	sourceCode string
	langConfig sandbox.LangConfig
	fileID     string // for compiled checkers or cached interpreted scripts
}

// prepareCustomChecker compiles (if compiled) or caches (if interpreted) the custom checker code.
func (w *Worker) prepareCustomChecker(ctx context.Context, checkerLang, checkerCode string, log *slog.Logger) (*customChecker, error) {
	if checkerLang == "" {
		checkerLang = "python"
	}
	langCfg, ok := sandbox.Languages[checkerLang]
	if !ok {
		return nil, fmt.Errorf("unsupported checker language: %s", checkerLang)
	}

	cc := &customChecker{
		language:   checkerLang,
		sourceCode: checkerCode,
		langConfig: langCfg,
	}

	cacheKey := compileCacheKey("checker:"+checkerLang, checkerCode)
	if cachedID, hit := w.cc.get(cacheKey); hit {
		cc.fileID = cachedID
		log.Debug("custom checker cache hit", "lang", checkerLang, "fileID", cachedID)
		return cc, nil
	}

	if langCfg.Compiled {
		compileCmd := sandbox.BuildCompileCmd(langCfg, checkerCode)
		results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{compileCmd}})
		if err != nil {
			return nil, fmt.Errorf("compile checker: %w", err)
		}
		if len(results) == 0 || results[0].Status != sandbox.StatusAccepted {
			stderr := ""
			if len(results) > 0 {
				stderr = results[0].Files["stderr"]
			}
			return nil, fmt.Errorf("checker compilation failed: %s", stderr)
		}
		for _, fid := range results[0].FileIDs {
			cc.fileID = fid
			break
		}
		if cc.fileID != "" {
			w.cc.put(cacheKey, cc.fileID)
		}
	} else if !langCfg.StdinIsSource {
		cacheCmd := sandbox.BuildCacheSourceCmd(langCfg, checkerCode)
		results, err := w.judge.Run(ctx, sandbox.Request{Cmd: []sandbox.Cmd{cacheCmd}})
		if err == nil && len(results) == 1 && results[0].Status == sandbox.StatusAccepted {
			for _, fid := range results[0].FileIDs {
				cc.fileID = fid
				break
			}
			if cc.fileID != "" {
				w.cc.put(cacheKey, cc.fileID)
			}
		}
	}

	return cc, nil
}

// buildCheckerCmd constructs the sandbox execution command for the custom checker.
// Standard testlib protocol: checker <input_file> <output_file> <answer_file>
// We provide:
//
//	/w/input.txt (problem test input)
//	/w/user_output.txt (solution stdout)
//	/w/expected_output.txt (expected reference answer)
//
// Exit code 0 indicates Accepted.
// Exit code 1 (or other non-zero) indicates Wrong Answer.
// Crash/Timeout indicates Judge Error / Internal Error.
func (c *customChecker) buildCheckerCmd(input, expectedOutput, actualStdout string) sandbox.Cmd {
	cpuLimit := uint64(5_000_000_000) // 5 seconds CPU cap for custom checker
	clockLimit := sandbox.ClockLimit(cpuLimit)
	memLimit := uint64(512 << 20) // 512MB memory

	args := make([]string, 0, len(c.langConfig.RunArgs)+3)
	args = append(args, c.langConfig.RunArgs...)
	args = append(args, "/w/input.txt", "/w/user_output.txt", "/w/expected_output.txt")

	copyIn := make(map[string]sandbox.CopyIn)
	copyIn["/w/input.txt"] = sandbox.FileContent(input)
	copyIn["/w/user_output.txt"] = sandbox.FileContent(actualStdout)
	copyIn["/w/expected_output.txt"] = sandbox.FileContent(expectedOutput)

	if c.fileID != "" {
		target := c.langConfig.BinaryPath
		if !c.langConfig.Compiled {
			target = c.langConfig.SourcePath
		}
		copyIn[target] = sandbox.CachedFile(c.fileID)
	} else if !c.langConfig.Compiled {
		copyIn[c.langConfig.SourcePath] = sandbox.FileContent(c.sourceCode)
	}

	return sandbox.Cmd{
		Args:        args,
		Env:         c.langConfig.RunEnv,
		Files:       []sandbox.CmdFile{sandbox.Stdin(""), sandbox.Collector("stdout", 64<<10), sandbox.Collector("stderr", 64<<10)},
		CPULimit:    cpuLimit,
		ClockLimit:  clockLimit,
		MemoryLimit: memLimit,
		ProcLimit:   c.langConfig.RunProc,
		CopyIn:      copyIn,
		CopyOut:     []string{"stdout", "stderr"},
	}
}

// evaluateCustomChecker executes the checker and interprets its exit status.
func (w *Worker) evaluateCustomChecker(ctx context.Context, cc *customChecker, input, expected, actual string) (model.Verdict, string) {
	evalCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := cc.buildCheckerCmd(input, expected, actual)
	results, err := w.judge.Run(evalCtx, sandbox.Request{Cmd: []sandbox.Cmd{cmd}})
	if err != nil {
		return model.VerdictInternalError, fmt.Sprintf("checker runner failed: %v", err)
	}
	if len(results) == 0 {
		return model.VerdictInternalError, "no result from checker"
	}

	res := results[0]
	switch res.Status {
	case sandbox.StatusAccepted:
		if res.ExitStatus == 0 {
			return model.VerdictAccepted, ""
		}
		return model.VerdictWrongAnswer, res.Files["stderr"]
	case sandbox.StatusNonzeroExit:
		if res.ExitStatus == 1 {
			return model.VerdictWrongAnswer, res.Files["stderr"]
		}
		// Any other exit status (e.g. 2, 3 or crash) is also treated as wrong answer or judge error
		msg := strings.TrimSpace(res.Files["stderr"])
		if msg == "" {
			msg = fmt.Sprintf("checker exited with code %d", res.ExitStatus)
		}
		return model.VerdictWrongAnswer, msg
	case sandbox.StatusTimeLimitEx:
		return model.VerdictInternalError, "checker timed out"
	case sandbox.StatusMemoryLimitEx:
		return model.VerdictInternalError, "checker exceeded memory limit"
	default:
		return model.VerdictInternalError, fmt.Sprintf("checker failure (%s): %s", res.Status, res.Files["stderr"])
	}
}
