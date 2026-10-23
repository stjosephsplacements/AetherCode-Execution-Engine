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

func (s *Stream) ack(ctx context.Context, msgID string) {
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	s.rdb.XAck(ackCtx, s.stream, s.group, msgID) //nolint:errcheck // best-effort: ACK failure leaves msg in PEL for reclaimer
	ackCancel()
}

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
					s.ack(ctx, msg.ID)
					continue
				}

				if err := handler(ctx, subID); err != nil {
					slog.Error("queue: handler error", "submission_id", subID, "err", err)
					continue
				}

				// Use a fresh context for the ACK so a cancelled shutdown ctx does not
				// silently drop the ACK and leave the message in the PEL.
				s.ack(ctx, msg.ID)
			}
		}
	}
}

// DualConsumer reads from priority streams with weighted fair queuing.
// Priority tiers:
// - runStream: interactive user runs (P0)
// - submitStream: formal contest/assessment submissions (P1)
// - bulkStream: bulk re-evaluations and batch jobs (P2)
//
// submitWeight controls how many submit messages to try in the cycle.
// bulkWeight controls how many bulk messages to try in the cycle.
type DualConsumer struct {
	rdb              *redis.Client
	submitStream     string
	runStream        string
	bulkStream       string
	group            string
	submitWeight     int
	bulkWeight       int
	blockReadTimeout time.Duration
	reclaimMinIdle   time.Duration // how long a message must be idle before reclaim; defaults to 30s
}

func NewDualConsumer(rdb *redis.Client, submitStream, runStream, group string, submitWeight int, blockReadTimeout time.Duration) *DualConsumer {
	return NewTieredConsumer(rdb, submitStream, runStream, "", group, submitWeight, 1, blockReadTimeout)
}

// NewTieredConsumer constructs a consumer reading up to three priority streams.
func NewTieredConsumer(rdb *redis.Client, submitStream, runStream, bulkStream, group string, submitWeight, bulkWeight int, blockReadTimeout time.Duration) *DualConsumer {
	if blockReadTimeout <= 0 {
		blockReadTimeout = 500 * time.Millisecond
	}
	if submitWeight <= 0 {
		submitWeight = 3
	}
	if bulkWeight <= 0 {
		bulkWeight = 1
	}
	return &DualConsumer{
		rdb:              rdb,
		submitStream:     submitStream,
		runStream:        runStream,
		bulkStream:       bulkStream,
		group:            group,
		submitWeight:     submitWeight,
		bulkWeight:       bulkWeight,
		blockReadTimeout: blockReadTimeout,
		reclaimMinIdle:   30 * time.Second,
	}
}

func (dc *DualConsumer) ack(ctx context.Context, stream, msgID string) {
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	dc.rdb.XAck(ackCtx, stream, dc.group, msgID) //nolint:errcheck // best-effort: ACK failure leaves msg in PEL for reclaimer
	ackCancel()
}

// streams returns the slice of configured stream names.
func (dc *DualConsumer) streams() []string {
	if dc.bulkStream != "" {
		return []string{dc.submitStream, dc.runStream, dc.bulkStream}
	}
	return []string{dc.submitStream, dc.runStream}
}

// CreateGroups idempotently creates consumer groups on all configured streams.
func (dc *DualConsumer) CreateGroups(ctx context.Context) error {
	for _, stream := range dc.streams() {
		err := dc.rdb.XGroupCreateMkStream(ctx, stream, dc.group, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("queue: create group on %s: %w", stream, err)
		}
	}
	return nil
}

// Consume implements weighted fair queuing across priority streams:
// try submitWeight messages from submitStream, 1 from runStream, and bulkWeight from bulkStream.
// If all streams are empty, block on all streams before retrying.
func (dc *DualConsumer) Consume(ctx context.Context, consumer string, handler func(ctx context.Context, submissionID string) error) {
	for {
		if ctx.Err() != nil {
			return
		}

		processed := false

		// Try submit stream (high priority: P1 contest / assessment)
		for range dc.submitWeight {
			if dc.consumeOne(ctx, dc.submitStream, consumer, handler) {
				processed = true
			} else {
				break
			}
		}

		// Try run stream (interactive user runs: P0)
		if dc.consumeOne(ctx, dc.runStream, consumer, handler) {
			processed = true
		}

		// Try bulk stream (bulk re-evaluations: P2)
		if dc.bulkStream != "" {
			for range dc.bulkWeight {
				if dc.consumeOne(ctx, dc.bulkStream, consumer, handler) {
					processed = true
				} else {
					break
				}
			}
		}

		if !processed {
			// All streams empty — block-wait across streams to avoid busy-loop
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
				dc.ack(ctx, stream, msg.ID)
				return true
			}

			if err := handler(ctx, subID); err != nil {
				slog.Error("queue: handler error", "stream", stream, "submission_id", subID, "err", err)
				return true // consumed but not ACKed — reclaimer will pick it up
			}

			dc.ack(ctx, stream, msg.ID)
			return true //nolint:staticcheck // one message per read by design; returning true re-triggers the loop for the remaining messages
		}
	}
	return false
}

func (dc *DualConsumer) blockRead(ctx context.Context, consumer string, handler func(ctx context.Context, submissionID string) error) {
	if ctx.Err() != nil {
		return
	}

	streamNames := dc.streams()
	streamsArg := make([]string, len(streamNames)*2)
	copy(streamsArg, streamNames)
	for i := range streamNames {
		streamsArg[len(streamNames)+i] = ">"
	}

	msgs, err := dc.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    dc.group,
		Consumer: consumer,
		Streams:  streamsArg,
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
				dc.ack(ctx, streamName, msg.ID)
				continue
			}
			if err := handler(ctx, subID); err != nil {
				slog.Error("queue: handler error", "stream", streamName, "submission_id", subID, "err", err)
				continue
			}
			dc.ack(ctx, streamName, msg.ID)
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
			for _, stream := range dc.streams() {
				dc.reclaimStream(ctx, stream, handler)
			}
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
			MinIdle:  dc.reclaimMinIdle,
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
				dc.ack(ctx, stream, msg.ID)
				continue
			}

			if deliveryCounts[msg.ID] >= maxDeliveryCount {
				slog.Error("reclaimer: dead-lettering poison message",
					"stream", stream, "submission_id", subID,
					"msg_id", msg.ID, "delivery_count", deliveryCounts[msg.ID])
				metrics.PoisonMessagesTotal.Inc()
				dlqCtx, dlqCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				dlqErr := dc.rdb.XAdd(dlqCtx, &redis.XAddArgs{
					Stream: "ac:dlq:poison",
					MaxLen: 10_000,
					Approx: true,
					Values: map[string]interface{}{
						"submission_id":  subID,
						"source_stream":  stream,
						"orig_msg_id":    msg.ID,
						"delivery_count": deliveryCounts[msg.ID],
						"reclaimed_at":   time.Now().UTC().Format(time.RFC3339),
					},
				}).Err()
				dlqCancel()
				if dlqErr != nil {
					slog.Error("reclaimer: failed to write poison message to DLQ, leaving unacknowledged for retry",
						"stream", stream, "submission_id", subID, "msg_id", msg.ID, "err", dlqErr)
					continue
				}
				dc.ack(ctx, stream, msg.ID)
				continue
			}

			slog.Info("reclaimer: reprocessing", "stream", stream, "submission_id", subID,
				"msg_id", msg.ID, "delivery_count", deliveryCounts[msg.ID])

			if err := handler(ctx, subID); err != nil {
				slog.Error("reclaimer: handler error", "stream", stream, "submission_id", subID, "err", err)
				continue
			}

			dc.ack(ctx, stream, msg.ID)
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
