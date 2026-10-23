package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

// PrepareAndEnqueue stores tests, publishes the QUEUED event, and enqueues
// the submission in a single Redis pipeline (1 round trip instead of 3).
func (p *PubSub) PrepareAndEnqueue(ctx context.Context, jobID string, set TestSet, evt eventlog.Event, streamKey string) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("pubsub: marshal event: %w", err)
	}

	pipe := p.rdb.Pipeline()

	if len(set.Tests) > 0 {
		key := "ac:tests:" + jobID
		fields := make([]any, 0, 10+2*len(set.Tests))
		fields = append(fields,
			"count", len(set.Tests),
			"cpu_ns", set.Limits.CPU,
			"mem_bytes", set.Limits.Memory,
			"checker", set.Checker,
			"checker_code", set.CheckerCode,
			"checker_language", set.CheckerLanguage,
			"interactor_code", set.InteractorCode,
			"interactor_language", set.InteractorLanguage,
			"callback_url", set.CallbackURL,
			"callback_secret", set.CallbackSecret,
		)
		if len(set.Files) > 0 {
			if filesJSON, err := json.Marshal(set.Files); err == nil {
				fields = append(fields, "files_json", string(filesJSON))
			}
		}
		if len(set.CompilerFlags) > 0 {
			if flagsJSON, err := json.Marshal(set.CompilerFlags); err == nil {
				fields = append(fields, "compiler_flags_json", string(flagsJSON))
			}
		}
		for i, t := range set.Tests {
			prefix := strconv.Itoa(i)
			fields = append(fields, prefix+":input", t.Input, prefix+":expected", t.ExpectedOutput)
		}
		pipe.HSet(ctx, key, fields...)
		pipe.Expire(ctx, key, testDataTTL)
	}

	pipe.RPush(ctx, logKey(evt.JobID), data)
	pipe.LTrim(ctx, logKey(evt.JobID), -500, -1) // keep last 500 entries
	pipe.Expire(ctx, logKey(evt.JobID), eventLogTTL)
	pipe.Publish(ctx, channelName(evt.JobID), data)

	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 100_000,
		Approx: true,
		Values: map[string]interface{}{"submission_id": jobID},
	})

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: prepare and enqueue: %w", err)
	}
	return nil
}

const eventLogTTL = 10 * time.Minute
const testDataTTL = 1 * time.Hour

type PubSub struct {
	rdb *redis.Client
	wg  sync.WaitGroup
}

func NewPubSub(rdb *redis.Client) *PubSub {
	return &PubSub{rdb: rdb}
}

// Wait blocks until all active Subscribe goroutines have exited.
// Call after srv.Shutdown to ensure sub.Close() completes before rdb.Close().
func (p *PubSub) Wait() { p.wg.Wait() }

func channelName(jobID string) string {
	return "ac:events:" + jobID
}

func logKey(jobID string) string {
	return "ac:events:" + jobID + ":log"
}

// PublishEvent publishes an event to the job's channel and appends it to the event log.
func (p *PubSub) PublishEvent(ctx context.Context, evt eventlog.Event) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("pubsub: marshal event: %w", err)
	}

	pipe := p.rdb.Pipeline()
	pipe.RPush(ctx, logKey(evt.JobID), data)
	pipe.LTrim(ctx, logKey(evt.JobID), -500, -1) // keep last 500 entries
	pipe.Expire(ctx, logKey(evt.JobID), eventLogTTL)
	pipe.Publish(ctx, channelName(evt.JobID), data)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: publish event: %w", err)
	}
	return nil
}

// GetEventLog returns all stored events for a job (for SSE replay).
func (p *PubSub) GetEventLog(ctx context.Context, jobID string) ([]eventlog.Event, error) {
	vals, err := p.rdb.LRange(ctx, logKey(jobID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("pubsub: get log: %w", err)
	}

	events := make([]eventlog.Event, 0, len(vals))
	for _, v := range vals {
		var evt eventlog.Event
		if err := json.Unmarshal([]byte(v), &evt); err != nil {
			continue
		}
		events = append(events, evt)
	}
	return events, nil
}

// Subscribe returns a channel that receives events for the given job.
// Close the context to unsubscribe.
func (p *PubSub) Subscribe(ctx context.Context, jobID string) <-chan eventlog.Event {
	ch := make(chan eventlog.Event, 16)
	sub := p.rdb.Subscribe(ctx, channelName(jobID))

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(ch)
		defer sub.Close() //nolint:errcheck // best-effort: unsubscribe when the SSE handler's goroutine exits

		msgCh := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var evt eventlog.Event
				if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
					continue
				}
				select {
				case ch <- evt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch
}

// GetTests retrieves test cases, run limits and checker from Redis in a single round trip.
func (p *PubSub) GetTests(ctx context.Context, jobID string) (TestSet, error) {
	key := "ac:tests:" + jobID
	fields, err := p.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return TestSet{}, fmt.Errorf("pubsub: get tests: %w", err)
	}

	countStr, ok := fields["count"]
	if !ok {
		return TestSet{}, fmt.Errorf("pubsub: no test count in %s", key)
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return TestSet{}, fmt.Errorf("pubsub: corrupt test count %q in %s", countStr, key)
	}
	if count < 0 || count > 10000 {
		return TestSet{}, fmt.Errorf("pubsub: invalid test count %d in %s", count, key)
	}

	tests := make([]TestData, count)
	for i := range count {
		prefix := strconv.Itoa(i)
		tests[i] = TestData{
			Input:          fields[prefix+":input"],
			ExpectedOutput: fields[prefix+":expected"],
		}
	}
	var limits sandbox.RunLimits
	// Absent on jobs enqueued before limits existed: zero keeps language defaults.
	if v := fields["cpu_ns"]; v != "" {
		limits.CPU, _ = strconv.ParseUint(v, 10, 64)
	}
	if v := fields["mem_bytes"]; v != "" {
		limits.Memory, _ = strconv.ParseUint(v, 10, 64)
	}
	var files map[string]string
	if fj := fields["files_json"]; fj != "" {
		_ = json.Unmarshal([]byte(fj), &files)
	}
	var compilerFlags []string
	if cfj := fields["compiler_flags_json"]; cfj != "" {
		_ = json.Unmarshal([]byte(cfj), &compilerFlags)
	}

	// Absent on jobs enqueued before the checker existed: empty means exact.
	return TestSet{
		Tests:              tests,
		Limits:             limits,
		Checker:            fields["checker"],
		CheckerCode:        fields["checker_code"],
		CheckerLanguage:    fields["checker_language"],
		InteractorCode:     fields["interactor_code"],
		InteractorLanguage: fields["interactor_language"],
		Files:              files,
		CompilerFlags:      compilerFlags,
		CallbackURL:        fields["callback_url"],
		CallbackSecret:     fields["callback_secret"],
	}, nil
}

type TestData struct {
	Input          string
	ExpectedOutput string
}

// TestSet is everything a job needs besides its source: the tests,
// the per-job resource limits and the output checker.
type TestSet struct {
	Tests              []TestData
	Limits             sandbox.RunLimits
	Checker            string            // empty means exact
	CheckerCode        string            // optional custom checker source code
	CheckerLanguage    string            // optional custom checker language ("cpp", "python")
	InteractorCode     string            // optional interactive interactor source code
	InteractorLanguage string            // optional interactive interactor language ("cpp", "python")
	Files              map[string]string // optional multi-file sources/headers
	CompilerFlags      []string          // optional whitelisted compiler flags
	CallbackURL        string            // optional webhook notification callback URL
	CallbackSecret     string            // optional HMAC signing secret for webhook
}
