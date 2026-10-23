package sandbox

import (
	"fmt"
	"path"
	"strings"
)

// AllowedCompilerFlags defines permitted compiler flags per language to prevent malicious or unsafe flags.
var AllowedCompilerFlags = map[string]map[string]bool{
	"c": {
		"-O0": true, "-O1": true, "-O2": true, "-O3": true, "-Os": true,
		"-std=c99": true, "-std=c11": true, "-std=c17": true,
		"-lm": true, "-Wall": true, "-Wextra": true, "-pedantic": true,
	},
	"cpp": {
		"-O0": true, "-O1": true, "-O2": true, "-O3": true, "-Os": true,
		"-std=c++11": true, "-std=c++14": true, "-std=c++17": true, "-std=c++20": true, "-std=c++23": true,
		"-lm": true, "-Wall": true, "-Wextra": true, "-pedantic": true,
	},
	"rust": {
		"-O": true, "--edition=2018": true, "--edition=2021": true,
	},
}

// ValidateCompilerFlags checks that every flag in flags is permitted for the given language.
func ValidateCompilerFlags(language string, flags []string) error {
	allowed, ok := AllowedCompilerFlags[language]
	if !ok {
		if len(flags) > 0 {
			return fmt.Errorf("custom compiler flags are not supported for %s", language)
		}
		return nil
	}
	for _, flag := range flags {
		if !allowed[flag] {
			return fmt.Errorf("flag %q is not permitted for %s", flag, language)
		}
	}
	return nil
}

// ValidateFiles validates multi-file submission maps.
// It ensures:
// 1. Total file count does not exceed maxFiles (e.g. 10).
// 2. Total combined size does not exceed maxTotalBytes (e.g. 128KB).
// 3. File paths are clean, relative, do not contain "..", and stay inside sandbox directory.
func ValidateFiles(files map[string]string, maxFiles int, maxTotalBytes int) error {
	if len(files) > maxFiles {
		return fmt.Errorf("too many files (maximum %d allowed)", maxFiles)
	}
	totalBytes := 0
	for name, content := range files {
		clean := path.Clean(name)
		if clean == "." || clean == "/" || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") || path.IsAbs(clean) {
			return fmt.Errorf("invalid file path %q", name)
		}
		totalBytes += len(content)
		if totalBytes > maxTotalBytes {
			return fmt.Errorf("total multi-file size exceeds %d bytes", maxTotalBytes)
		}
	}
	return nil
}
