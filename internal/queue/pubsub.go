package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
)

// PrepareAndEnqueue stores tests, publishes the QUEUED event, and enqueues
// the submission in a single Redis pipeline (1 round trip instead of 3).
func (p *PubSub) PrepareAndEnqueue(ctx context.Context, jobID string, tests []TestData, evt eventlog.Event, streamKey string) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("pubsub: marshal event: %w", err)
	}

	pipe := p.rdb.Pipeline()

	if len(tests) > 0 {
		key := "ac:tests:" + jobID
		pipe.HSet(ctx, key, "count", len(tests))
		for i, t := range tests {
			pipe.HSet(ctx, key, fmt.Sprintf("%d:input", i), t.Input)
			pipe.HSet(ctx, key, fmt.Sprintf("%d:expected", i), t.ExpectedOutput)
		}
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
		defer sub.Close()

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

// StoreTests stores test cases in a Redis hash for the worker to read.
func (p *PubSub) StoreTests(ctx context.Context, jobID string, tests []TestData) error {
	key := "ac:tests:" + jobID
	pipe := p.rdb.Pipeline()
	pipe.HSet(ctx, key, "count", len(tests))
	for i, t := range tests {
		pipe.HSet(ctx, key, fmt.Sprintf("%d:input", i), t.Input)
		pipe.HSet(ctx, key, fmt.Sprintf("%d:expected", i), t.ExpectedOutput)
	}
	pipe.Expire(ctx, key, testDataTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: store tests: %w", err)
	}
	return nil
}

// GetTests retrieves test cases from Redis in a single round trip.
func (p *PubSub) GetTests(ctx context.Context, jobID string) ([]TestData, error) {
	key := "ac:tests:" + jobID
	fields, err := p.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("pubsub: get tests: %w", err)
	}

	countStr, ok := fields["count"]
	if !ok {
		return nil, fmt.Errorf("pubsub: no test count in %s", key)
	}

	var count int
	if n, err := fmt.Sscanf(countStr, "%d", &count); n != 1 || err != nil {
		return nil, fmt.Errorf("pubsub: corrupt test count %q in %s", countStr, key)
	}
	if count < 0 || count > 10000 {
		return nil, fmt.Errorf("pubsub: invalid test count %d in %s", count, key)
	}

	tests := make([]TestData, count)
	for i := range count {
		tests[i] = TestData{
			Input:          fields[fmt.Sprintf("%d:input", i)],
			ExpectedOutput: fields[fmt.Sprintf("%d:expected", i)],
		}
	}
	return tests, nil
}

type TestData struct {
	Input          string
	ExpectedOutput string
}
