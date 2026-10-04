package eventlog

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIsTerminal(t *testing.T) {
	if !New("job", EventVerdict, nil).IsTerminal() {
		t.Fatal("VERDICT should be terminal")
	}
	for _, typ := range []EventType{EventQueued, EventCompiling, EventRunning} {
		if New("job", typ, nil).IsTerminal() {
			t.Fatalf("%s should not be terminal", typ)
		}
	}
}

func TestMarshalSSE(t *testing.T) {
	e := Event{ID: "j:1-2", JobID: "j", Type: EventQueued, Timestamp: 123}
	out := string(e.MarshalSSE())

	if !strings.HasPrefix(out, "id: j:1-2\nevent: QUEUED\ndata: ") {
		t.Fatalf("bad SSE prefix: %q", out)
	}
	if !strings.HasSuffix(out, "\n\n") {
		t.Fatalf("SSE frame must end with a blank line: %q", out)
	}

	// The data payload must be valid JSON carrying the job id and type.
	dataLine := strings.TrimPrefix(strings.SplitN(out, "\n", 4)[2], "data: ")
	var payload map[string]any
	if err := json.Unmarshal([]byte(dataLine), &payload); err != nil {
		t.Fatalf("data payload is not JSON: %v\npayload=%q", err, dataLine)
	}
	if payload["job_id"] != "j" || payload["type"] != "QUEUED" {
		t.Fatalf("unexpected payload: %v", payload)
	}
}

func TestMarshalSSEOmitsNilData(t *testing.T) {
	e := Event{ID: "j:1-3", JobID: "j", Type: EventRunning, Timestamp: 5}
	out := string(e.MarshalSSE())
	if strings.Contains(out, `"data":`) {
		t.Fatalf("nil Data should be omitted from JSON, got: %q", out)
	}
}

func TestNew(t *testing.T) {
	e := New("job123", EventRunning, "payload")
	if e.JobID != "job123" {
		t.Fatalf("JobID = %q, want job123", e.JobID)
	}
	if e.Type != EventRunning {
		t.Fatalf("Type = %q, want RUNNING", e.Type)
	}
	if e.Data != "payload" {
		t.Fatalf("Data = %v, want payload", e.Data)
	}
	if e.Timestamp == 0 {
		t.Fatal("Timestamp should be set")
	}
	// ID encodes the job id and must be unique across calls.
	if !strings.HasPrefix(e.ID, "job123:") {
		t.Fatalf("ID should start with job id, got %q", e.ID)
	}
	a := New("x", EventQueued, nil)
	b := New("x", EventQueued, nil)
	if a.ID == b.ID {
		t.Fatalf("IDs should be unique, both %q", a.ID)
	}
}
