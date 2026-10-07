package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
)

type Stream struct {
	rdb           *redis.Client
	stream        string
	group         string
	blockDuration time.Duration
}

func NewStream(rdb *redis.Client, stream, group string) *Stream {
	return &Stream{
		rdb:           rdb,
		stream:        stream,
		group:         group,
		blockDuration: 5 * time.Second,
	}
}

// StreamName returns the Redis stream key.
func (s *Stream) StreamName() string { return s.stream }

// CreateGroup idempotently creates the consumer group and stream.
func (s *Stream) CreateGroup(ctx context.Context) error {
	err := s.rdb.XGroupCreateMkStream(ctx, s.stream, s.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("queue: create group: %w", err)
	}
	return nil
}

// Enqueue adds a submission ID to the stream.
func (s *Stream) Enqueue(ctx context.Context, submissionID string) error {
	_, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		MaxLen: 100_000,
		Approx: true,
		Values: map[string]interface{}{
			"submission_id": submissionID,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("queue: enqueue: %w", err)
	}
	return nil
}

// Consume reads messages in a loop, calling handler for each.
// It blocks until ctx is cancelled. Each message is ACKed after the handler returns nil.
func (s *Stream) Consume(ctx context.Context, consumer string, handler func(ctx context.Context, submissionID string) error) {
	for {
		if ctx.Err() != nil {
			return
		}

		msgs, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: consumer,
			Streams:  []string{s.stream, ">"},
			Count:    1,
			Block:    s.blockDuration,
		}).Result()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if err == redis.Nil {
				continue
			}
			slog.Error("queue: read error", "err", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range msgs {
			for _, msg := range stream.Messages {
				subID, ok := msg.Values["submission_id"].(string)
				if !ok {
					slog.Error("queue: invalid message, missing submission_id", "id", msg.ID)
					ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
					s.rdb.XAck(ackCtx, s.stream, s.group, msg.ID)
					ackCancel()
					continue
				}

				if err := handler(ctx, subID); err != nil {
					slog.Error("queue: handler error", "submission_id", subID, "err", err)
					continue
				}

				// Use a fresh context for the ACK so a cancelled shutdown ctx does not
				// silently drop the ACK and leave the message in the PEL.
				ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				s.rdb.XAck(ackCtx, s.stream, s.group, msg.ID)
				ackCancel()
			}
		}
	}
}

// DualConsumer reads from two streams with weighted priority.
// submitWeight controls how many submit messages to try before one run message.
type DualConsumer struct {
	rdb              *redis.Client
	submitStream     string
	runStream        string
	group            string
	submitWeight     int
	blockReadTimeout time.Duration
}

func NewDualConsumer(rdb *redis.Client, submitStream, runStream, group string, submitWeight int, blockReadTimeout time.Duration) *DualConsumer {
	if blockReadTimeout <= 0 {
		blockReadTimeout = 500 * time.Millisecond
	}
	return &DualConsumer{
		rdb:              rdb,
		submitStream:     submitStream,
		runStream:        runStream,
		group:            group,
		submitWeight:     submitWeight,
		blockReadTimeout: blockReadTimeout,
	}
}

// CreateGroups idempotently creates consumer groups on both streams.
func (dc *DualConsumer) CreateGroups(ctx context.Context) error {
	for _, stream := range []string{dc.submitStream, dc.runStream} {
		err := dc.rdb.XGroupCreateMkStream(ctx, stream, dc.group, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("queue: create group on %s: %w", stream, err)
		}
	}
	return nil
}

// Consume implements weighted fair queuing: try submitWeight messages from
// the submit stream, then 1 from the run stream. If both are empty, block
// on submit for 2s before retrying.
func (dc *DualConsumer) Consume(ctx context.Context, consumer string, handler func(ctx context.Context, submissionID string) error) {
	for {
		if ctx.Err() != nil {
			return
		}

		processed := false

		// Try submit stream (high priority)
		for range dc.submitWeight {
			if dc.consumeOne(ctx, dc.submitStream, consumer, handler) {
				processed = true
			} else {
				break
			}
		}

		// Try run stream (lower priority)
		if dc.consumeOne(ctx, dc.runStream, consumer, handler) {
			processed = true
		}

		if !processed {
			// Both empty — block-wait on submit stream to avoid busy-loop
			dc.blockRead(ctx, consumer, handler)
		}
	}
}

func (dc *DualConsumer) consumeOne(ctx context.Context, stream, consumer string, handler func(ctx context.Context, submissionID string) error) bool {
	if ctx.Err() != nil {
		return false
	}

	msgs, err := dc.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    dc.group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    time.Millisecond, // near-instant, avoids Block:0 which means "forever"
	}).Result()

	if err != nil || len(msgs) == 0 {
		return false
	}

	for _, s := range msgs {
		for _, msg := range s.Messages {
			subID, ok := msg.Values["submission_id"].(string)
			if !ok {
				slog.Error("queue: invalid message", "stream", stream, "id", msg.ID)
				ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				dc.rdb.XAck(ackCtx, stream, dc.group, msg.ID)
				ackCancel()
				return true
			}

			if err := handler(ctx, subID); err != nil {
				slog.Error("queue: handler error", "stream", stream, "submission_id", subID, "err", err)
				return true // consumed but not ACKed — reclaimer will pick it up
			}

			ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			dc.rdb.XAck(ackCtx, stream, dc.group, msg.ID)
			ackCancel()
			return true //nolint:staticcheck // one message per read by design; returning true re-triggers the loop for the remaining messages
		}
	}
	return false
}

func (dc *DualConsumer) blockRead(ctx context.Context, consumer string, handler func(ctx context.Context, submissionID string) error) {
	if ctx.Err() != nil {
		return
	}

	msgs, err := dc.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    dc.group,
		Consumer: consumer,
		Streams:  []string{dc.submitStream, dc.runStream, ">", ">"},
		Count:    1,
		Block:    dc.blockReadTimeout,
	}).Result()

	if err != nil {
		if ctx.Err() != nil || err == redis.Nil {
			return
		}
		slog.Error("queue: block read error", "err", err)
		time.Sleep(time.Second)
		return
	}

	for _, s := range msgs {
		streamName := s.Stream
		for _, msg := range s.Messages {
			subID, ok := msg.Values["submission_id"].(string)
			if !ok {
				ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				dc.rdb.XAck(ackCtx, streamName, dc.group, msg.ID)
				ackCancel()
				continue
			}
			if err := handler(ctx, subID); err != nil {
				slog.Error("queue: handler error", "stream", streamName, "submission_id", subID, "err", err)
				continue
			}
			ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			dc.rdb.XAck(ackCtx, streamName, dc.group, msg.ID)
			ackCancel()
		}
	}
}

// StartReclaimer runs a background goroutine that reclaims stuck messages
// from both streams every 30s using XAUTOCLAIM. Messages idle for > 30s
// are re-processed through the handler.
func (dc *DualConsumer) StartReclaimer(ctx context.Context, wg *sync.WaitGroup, handler func(ctx context.Context, submissionID string) error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			panicked := dc.runReclaimer(ctx, handler)
			if !panicked || ctx.Err() != nil {
				return
			}
			slog.Warn("restarting reclaimer after panic")
			time.Sleep(time.Second)
		}
	}()
	slog.Info("reclaimer started", "interval", "30s")
}

func (dc *DualConsumer) runReclaimer(ctx context.Context, handler func(ctx context.Context, submissionID string) error) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("reclaimer panic recovered", "panic", r)
			panicked = true
		}
	}()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			dc.reclaimStream(ctx, dc.submitStream, handler)
			dc.reclaimStream(ctx, dc.runStream, handler)
		}
	}
}

const maxDeliveryCount = 5

func (dc *DualConsumer) reclaimStream(ctx context.Context, stream string, handler func(ctx context.Context, submissionID string) error) {
	cursor := "0-0"
	for {
		if ctx.Err() != nil {
			return
		}

		msgs, newCursor, err := dc.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    dc.group,
			Consumer: "reclaimer",
			MinIdle:  30 * time.Second,
			Start:    cursor,
			Count:    50,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("reclaimer: xautoclaim error", "stream", stream, "err", err)
			return
		}

		if len(msgs) == 0 {
			if newCursor == "0-0" {
				return // full PEL scanned, nothing to reclaim
			}
			cursor = newCursor
			continue // advance to the next window even if this one was empty
		}

		// Query delivery counts only for the exact IDs returned by XAutoClaim.
		// Using XPENDING with a range is unreliable when the reclaimer has residual
		// PEL entries from prior interrupted cycles. Instead, fetch the full
		// reclaimer PEL so delivery counts for all owned messages are available,
		// including those left over from interrupted cycles. The existing
		// maxDeliveryCount guard then works correctly for every message.
		pending, err := dc.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   stream,
			Group:    dc.group,
			Consumer: "reclaimer",
			Start:    "-",
			End:      "+",
			Count:    1000, // covers worst-case accumulated PEL for this consumer
		}).Result()
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("reclaimer: xpending error, skipping batch to avoid reprocessing poison messages", "stream", stream, "err", err)
			}
			return // abort this reclaim cycle; retry on next ticker
		}
		deliveryCounts := make(map[string]int64, len(pending))
		for _, p := range pending {
			deliveryCounts[p.ID] = p.RetryCount
		}

		for _, msg := range msgs {
			subID, ok := msg.Values["submission_id"].(string)
			if !ok {
				ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				dc.rdb.XAck(ackCtx, stream, dc.group, msg.ID)
				ackCancel()
				continue
			}

			if deliveryCounts[msg.ID] >= maxDeliveryCount {
				slog.Error("reclaimer: dead-lettering poison message",
					"stream", stream, "submission_id", subID,
					"msg_id", msg.ID, "delivery_count", deliveryCounts[msg.ID])
				metrics.PoisonMessagesTotal.Inc()
				ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				dc.rdb.XAck(ackCtx, stream, dc.group, msg.ID)
				ackCancel()
				continue
			}

			slog.Info("reclaimer: reprocessing", "stream", stream, "submission_id", subID,
				"msg_id", msg.ID, "delivery_count", deliveryCounts[msg.ID])

			if err := handler(ctx, subID); err != nil {
				slog.Error("reclaimer: handler error", "stream", stream, "submission_id", subID, "err", err)
				continue
			}

			ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			dc.rdb.XAck(ackCtx, stream, dc.group, msg.ID)
			ackCancel()
		}

		if newCursor == "0-0" {
			return
		}
		cursor = newCursor
	}
}

// DrainLegacyStream reads remaining messages from the old single stream
// and processes them, then returns when the stream is empty.
func DrainLegacyStream(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client, oldStream, group string, handler func(ctx context.Context, submissionID string) error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("legacy drain panic recovered", "panic", r)
			}
		}()
		consumer := "legacy-drain"

		// Ensure group exists
		rdb.XGroupCreateMkStream(ctx, oldStream, group, "0")

		emptyReads := 0
		for {
			if ctx.Err() != nil {
				return
			}

			msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Streams:  []string{oldStream, ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if err == redis.Nil {
					emptyReads++
					if emptyReads >= 2 {
						slog.Info("legacy stream drained", "stream", oldStream)
						return
					}
					continue
				}
				time.Sleep(time.Second)
				continue
			}

			got := false
			for _, s := range msgs {
				for _, msg := range s.Messages {
					got = true
					subID, ok := msg.Values["submission_id"].(string)
					if !ok {
						rdb.XAck(ctx, oldStream, group, msg.ID)
						continue
					}
					if err := handler(ctx, subID); err != nil {
						slog.Error("legacy drain: handler error", "submission_id", subID, "err", err)
						continue
					}
					rdb.XAck(ctx, oldStream, group, msg.ID)
				}
			}
			if !got {
				emptyReads++
				if emptyReads >= 2 {
					slog.Info("legacy stream drained", "stream", oldStream)
					return
				}
			} else {
				emptyReads = 0
			}
		}
	}()
}
