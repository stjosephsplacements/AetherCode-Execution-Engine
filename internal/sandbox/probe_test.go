package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLangBinaries(t *testing.T) {
	t.Parallel()
	if got := Languages["c"].Binaries(); len(got) != 1 || got[0] != "/usr/bin/gcc" {
		t.Errorf("c binaries = %v, want only gcc: the compiled program is not a toolchain binary", got)
	}
	if got := Languages["java"].Binaries(); len(got) != 2 || got[0] != "/usr/bin/javac" || got[1] != "/usr/bin/java" {
		t.Errorf("java binaries = %v, want javac and java", got)
	}
	if got := Languages["python"].Binaries(); len(got) != 1 || got[0] != "/usr/bin/python3" {
		t.Errorf("python binaries = %v, want python3", got)
	}
}

func TestProbeLanguages(t *testing.T) {
	t.Parallel()
	missing := map[string]bool{"/usr/bin/rustc": true, "/usr/bin/php": true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		results := make([]Result, len(req.Cmd))
		for i, cmd := range req.Cmd {
			results[i] = Result{Status: StatusAccepted}
			if missing[cmd.Args[2]] {
				results[i] = Result{Status: StatusNonzeroExit, ExitStatus: 1}
			}
		}
		_ = json.NewEncoder(w).Encode(results)
	}))
	defer server.Close()

	available, err := NewClient(server.URL, "t").ProbeLanguages(context.Background())
	if err != nil {
		t.Fatalf("ProbeLanguages: %v", err)
	}
	if available["rust"] || available["php"] {
		t.Errorf("rust = %v, php = %v, want both unavailable", available["rust"], available["php"])
	}
	if !available["c"] || !available["java"] || !available["python"] {
		t.Errorf("c, java and python = %v %v %v, want all available", available["c"], available["java"], available["python"])
	}
	if len(available) != len(Languages) {
		t.Errorf("len = %d, want an entry for each of the %d languages", len(available), len(Languages))
	}
}

func TestProbeLanguagesReportsASandboxFailure(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer server.Close()
	if _, err := NewClient(server.URL, "t").ProbeLanguages(context.Background()); err == nil {
		t.Fatal("ProbeLanguages returned no error for a failing sandbox")
	}
}
