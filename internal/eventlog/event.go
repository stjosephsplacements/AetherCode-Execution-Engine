package eventlog

import (
	"encoding/json"
	"strconv"
	"sync/atomic"
	"time"
)

type EventType string

const (
	EventQueued     EventType = "QUEUED"
	EventCompiling  EventType = "COMPILING"
	EventRunning    EventType = "RUNNING"
	EventTestResult EventType = "TEST_RESULT"
	EventVerdict    EventType = "VERDICT"
)

type Event struct {
	ID        string    `json:"id"`
	JobID     string    `json:"job_id"`
	Type      EventType `json:"type"`
	Data      any       `json:"data,omitempty"`
	Timestamp int64     `json:"ts"`
}

func (e Event) IsTerminal() bool {
	return e.Type == EventVerdict
}

func (e Event) MarshalSSE() []byte {
	data, _ := json.Marshal(e)
	// Pre-size: "id: " + id + "\nevent: " + type + "\ndata: " + data + "\n\n"
	buf := make([]byte, 0, 4+len(e.ID)+8+len(e.Type)+7+len(data)+2)
	buf = append(buf, "id: "...)
	buf = append(buf, e.ID...)
	buf = append(buf, "\nevent: "...)
	buf = append(buf, e.Type...)
	buf = append(buf, "\ndata: "...)
	buf = append(buf, data...)
	buf = append(buf, "\n\n"...)
	return buf
}

var (
	// Use nanoseconds to make same-millisecond collisions astronomically unlikely.
	processEpoch = time.Now().UnixNano()
	seqCounter   atomic.Int64
)

func New(jobID string, typ EventType, data any) Event {
	seq := seqCounter.Add(1)
	id := jobID + ":" + strconv.FormatInt(processEpoch, 10) + "-" + strconv.FormatInt(seq, 10)
	return Event{
		ID:        id,
		JobID:     jobID,
		Type:      typ,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}
}
