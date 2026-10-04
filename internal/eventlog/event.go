package eventlog

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

type EventType string

const (
	EventQueued    EventType = "QUEUED"
	EventCompiling EventType = "COMPILING"
	EventRunning   EventType = "RUNNING"
	EventVerdict   EventType = "VERDICT"
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
	return []byte(fmt.Sprintf("id: %s\nevent: %s\ndata: %s\n\n", e.ID, e.Type, data))
}

var (
	// Use nanoseconds to make same-millisecond collisions astronomically unlikely.
	processEpoch = time.Now().UnixNano()
	seqCounter   atomic.Int64
)

func New(jobID string, typ EventType, data any) Event {
	seq := seqCounter.Add(1)
	return Event{
		ID:        fmt.Sprintf("%s:%d-%d", jobID, processEpoch, seq),
		JobID:     jobID,
		Type:      typ,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}
}
