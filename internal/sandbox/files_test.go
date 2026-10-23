package sandbox

import (
	"testing"
)

func TestValidateCompilerFlags(t *testing.T) {
	tests := []struct {
		lang    string
		flags   []string
		wantErr bool
	}{
		{"cpp", []string{"-O3", "-std=c++20"}, false},
		{"c", []string{"-O2", "-Wall"}, false},
		{"c", []string{"-O3", "-std=c11", "-lm"}, false},
		{"rust", []string{"-O", "--edition=2021"}, false},
		{"cpp", []string{"-funsafe-flag"}, true},
		{"python", []string{"-O3"}, true},
		{"cpp", []string{"-I/etc/shadow"}, true},
	}

	for _, tt := range tests {
		err := ValidateCompilerFlags(tt.lang, tt.flags)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateCompilerFlags(%q, %v) error = %v, wantErr = %v", tt.lang, tt.flags, err, tt.wantErr)
		}
	}
}

func TestValidateFiles(t *testing.T) {
	valid := map[string]string{
		"solution.h": "#pragma once\nint solve();",
		"util.cpp":   "int util() { return 1; }",
	}
	if err := ValidateFiles(valid, 10, 128<<10); err != nil {
		t.Fatalf("expected valid files, got: %v", err)
	}

	tooMany := make(map[string]string)
	for i := range 11 {
		tooMany[string(rune('a'+i))+".h"] = "ok"
	}
	if err := ValidateFiles(tooMany, 10, 128<<10); err == nil {
		t.Errorf("expected error for >10 files, got nil")
	}

	traversal := map[string]string{
		"../secret.txt": "evil",
	}
	if err := ValidateFiles(traversal, 10, 128<<10); err == nil {
		t.Errorf("expected error for path traversal, got nil")
	}

	absolute := map[string]string{
		"/etc/hosts": "127.0.0.1 localhost",
	}
	if err := ValidateFiles(absolute, 10, 128<<10); err == nil {
		t.Errorf("expected error for absolute path, got nil")
	}
}
