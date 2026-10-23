package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestPrepareAndEnqueue_AtomicPipeline(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	tests := []TestData{
		{Input: "1 2", ExpectedOutput: "3"},
		{Input: "4 5", ExpectedOutput: "9"},
	}
	limits := sandbox.RunLimits{CPU: 2_000_000_000, Memory: 256 << 20}
	evt := eventlog.New("job-abc", eventlog.EventQueued, nil)

	if err := ps.PrepareAndEnqueue(ctx, "job-abc", TestSet{Tests: tests, Limits: limits}, evt, "ac:test:stream"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	// Tests should be stored in Redis
	gotSet, err := ps.GetTests(ctx, "job-abc")
	gotTests, gotLimits := gotSet.Tests, gotSet.Limits
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if len(gotTests) != 2 {
		t.Fatalf("len(tests) = %d, want 2", len(gotTests))
	}
	if gotTests[0].Input != "1 2" || gotTests[0].ExpectedOutput != "3" {
		t.Errorf("test[0] = %+v", gotTests[0])
	}
	if gotTests[1].Input != "4 5" || gotTests[1].ExpectedOutput != "9" {
		t.Errorf("test[1] = %+v", gotTests[1])
	}

	// Limits should round-trip
	if gotLimits.CPU != limits.CPU {
		t.Errorf("CPU limit = %d, want %d", gotLimits.CPU, limits.CPU)
	}
	if gotLimits.Memory != limits.Memory {
		t.Errorf("Memory limit = %d, want %d", gotLimits.Memory, limits.Memory)
	}

	// Stream entry should exist
	n, err := rdb.XLen(ctx, "ac:test:stream").Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 1 {
		t.Fatalf("stream length = %d, want 1", n)
	}

	// Event log should have the QUEUED event
	events, err := ps.GetEventLog(ctx, "job-abc")
	if err != nil {
		t.Fatalf("GetEventLog: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event log len = %d, want 1", len(events))
	}
	if events[0].Type != eventlog.EventQueued {
		t.Errorf("event type = %q, want QUEUED", events[0].Type)
	}
}

func TestGetTests_RoundTrip(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	tests := []TestData{
		{Input: "hello", ExpectedOutput: "world"},
	}
	evt := eventlog.New("job-xyz", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-xyz", TestSet{Tests: tests}, evt, "ac:stream:x"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	gotSet, err := ps.GetTests(ctx, "job-xyz")
	got := gotSet.Tests
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if len(got) != 1 || got[0].Input != "hello" || got[0].ExpectedOutput != "world" {
		t.Errorf("unexpected tests: %+v", got)
	}
}

func TestGetTests_ZeroLimitsPreservesDefault(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	evt := eventlog.New("job-lim", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-lim", TestSet{Tests: []TestData{{Input: "x", ExpectedOutput: "y"}}}, evt, "s"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	limSet, err := ps.GetTests(ctx, "job-lim")
	limits := limSet.Limits
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if limits.CPU != 0 || limits.Memory != 0 {
		t.Errorf("expected zero limits, got %+v", limits)
	}
}

func TestPublishEvent_GetEventLog(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	jobID := "job-evlog"
	types := []eventlog.EventType{eventlog.EventQueued, eventlog.EventCompiling, eventlog.EventRunning, eventlog.EventVerdict}
	for _, typ := range types {
		if err := ps.PublishEvent(ctx, eventlog.New(jobID, typ, nil)); err != nil {
			t.Fatalf("PublishEvent(%s): %v", typ, err)
		}
	}

	events, err := ps.GetEventLog(ctx, jobID)
	if err != nil {
		t.Fatalf("GetEventLog: %v", err)
	}
	if len(events) != len(types) {
		t.Fatalf("got %d events, want %d", len(events), len(types))
	}
	for i, ev := range events {
		if ev.Type != types[i] {
			t.Errorf("event[%d].Type = %q, want %q", i, ev.Type, types[i])
		}
		if ev.JobID != jobID {
			t.Errorf("event[%d].JobID = %q, want %q", i, ev.JobID, jobID)
		}
	}
}

func TestSubscribe_ReceivesPublishedEvents(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ps := NewPubSub(rdb)
	jobID := "job-sub"

	ch := ps.Subscribe(ctx, jobID)

	// Publish in a goroutine after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := ps.PublishEvent(context.Background(), eventlog.New(jobID, eventlog.EventVerdict, map[string]any{"verdict": "accepted"})); err != nil {
			_ = err // best-effort in goroutine; test will fail if event not received
		}
	}()

	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before receiving event")
		}
		if ev.Type != eventlog.EventVerdict {
			t.Errorf("event type = %q, want VERDICT", ev.Type)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for subscribed event")
	}

	ps.Wait()
}

func TestGetEventLog_EmptyJob(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	events, err := ps.GetEventLog(ctx, "nonexistent-job")
	if err != nil {
		t.Fatalf("GetEventLog on empty job: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events for empty job, got %d", len(events))
	}
}

func TestPrepareAndEnqueue_NoTests(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	// No tests passed, event + stream entry only
	evt := eventlog.New("job-notests", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-notests", TestSet{}, evt, "ac:stream:s"); err != nil {
		t.Fatalf("PrepareAndEnqueue with no tests: %v", err)
	}

	n, _ := rdb.XLen(ctx, "ac:stream:s").Result()
	if n != 1 {
		t.Fatalf("stream length = %d, want 1", n)
	}

	evts, _ := ps.GetEventLog(ctx, "job-notests")
	if len(evts) != 1 {
		t.Fatalf("event log len = %d, want 1", len(evts))
	}
}

func TestGetEventLog_Replay(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	jobID := "job-replay"
	// Simulate a completed job: QUEUED → RUNNING → VERDICT
	for _, typ := range []eventlog.EventType{eventlog.EventQueued, eventlog.EventRunning, eventlog.EventVerdict} {
		if err := ps.PublishEvent(ctx, eventlog.New(jobID, typ, nil)); err != nil {
			t.Fatalf("PublishEvent %s: %v", typ, err)
		}
	}

	// SSE handler reconnects and replays
	events, err := ps.GetEventLog(ctx, jobID)
	if err != nil {
		t.Fatalf("GetEventLog replay: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("replay: got %d events, want 3", len(events))
	}
	if !events[2].IsTerminal() {
		t.Errorf("last event should be terminal (VERDICT)")
	}
}

func TestPrepareAndEnqueue_MultipleEnqueues_IndependentStreams(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	for i, stream := range []string{"ac:stream:a", "ac:stream:b", "ac:stream:c"} {
		jobID := "job-multi-" + string(rune('0'+i))
		evt := eventlog.New(jobID, eventlog.EventQueued, nil)
		if err := ps.PrepareAndEnqueue(ctx, jobID, TestSet{}, evt, stream); err != nil {
			t.Fatalf("PrepareAndEnqueue[%d]: %v", i, err)
		}
	}

	for _, stream := range []string{"ac:stream:a", "ac:stream:b", "ac:stream:c"} {
		n, _ := rdb.XLen(ctx, stream).Result()
		if n != 1 {
			t.Errorf("stream %q has %d entries, want 1", stream, n)
		}
	}
}

func TestSubscribe_Wait_DrainsSafely(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	ps := NewPubSub(rdb)

	// Start a subscription then immediately cancel
	_ = ps.Subscribe(ctx, "job-drain")
	cancel()

	// Wait must return without hanging
	done := make(chan struct{})
	go func() {
		ps.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PubSub.Wait() hung after context cancel")
	}
}

// Regression: XReadGroup returns goredis.Nil (not an error) when the stream is empty;
// the consumer must treat that as "nothing to do" and not log an error.
func TestDualConsumer_EmptyStreams_NoPanic(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	dc := NewDualConsumer(rdb, "ac:empty:sub", "ac:empty:run", "grp", 3, 200*time.Millisecond)
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	// Should not panic; exits when ctx times out
	dc.Consume(ctx, "w0", func(_ context.Context, id string) error {
		t.Errorf("handler called unexpectedly with id=%q", id)
		return nil
	})
}

// Check that Consume for Stream (non-dual) also works correctly.
func TestStream_Consume_SingleMessage(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := NewStream(rdb, "ac:sc:stream", "grp")
	if err := s.CreateGroup(ctx); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := s.Enqueue(ctx, "my-job"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	received := make(chan string, 1)
	go func() {
		s.Consume(ctx, "w0", func(_ context.Context, id string) error {
			received <- id
			cancel()
			return nil
		})
	}()

	select {
	case id := <-received:
		if id != "my-job" {
			t.Errorf("received id=%q, want %q", id, "my-job")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPrepareAndGetTests_RoundTripsChecker(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	set := TestSet{
		Tests:   []TestData{{Input: "1", ExpectedOutput: "1.0"}, {Input: "2", ExpectedOutput: "2.0"}},
		Limits:  sandbox.RunLimits{CPU: 2_000_000_000, Memory: 256 << 20},
		Checker: "float",
	}
	evt := eventlog.New("job-chk", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-chk", set, evt, "ac:stream:c"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	got, err := ps.GetTests(ctx, "job-chk")
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if got.Checker != "float" {
		t.Errorf("Checker = %q, want float", got.Checker)
	}
	if got.Limits != set.Limits {
		t.Errorf("Limits = %+v, want %+v", got.Limits, set.Limits)
	}
	if len(got.Tests) != 2 || got.Tests[0] != set.Tests[0] || got.Tests[1] != set.Tests[1] {
		t.Errorf("Tests = %+v, want %+v", got.Tests, set.Tests)
	}
}

func TestGetTests_MissingCheckerIsEmpty(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()
	ps := NewPubSub(rdb)

	if err := rdb.HSet(ctx, "ac:tests:job-old", "count", 1, "0:input", "a", "0:expected", "b").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	got, err := ps.GetTests(ctx, "job-old")
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if got.Checker != "" {
		t.Errorf("Checker = %q, want empty", got.Checker)
	}
}

// TestCallbackSecret_EncryptDecryptRoundTrip verifies that PrepareAndEnqueue encrypts
// the callback_secret and GetTests returns the original plaintext when both use the
// same master key.
func TestCallbackSecret_EncryptDecryptRoundTrip(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	// 32-byte test key (not a real secret)
	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i + 1)
	}
	ps := NewPubSubWithKey(rdb, masterKey)

	secret := "super-secret-hmac-key-12345"
	set := TestSet{
		Tests:          []TestData{{Input: "a", ExpectedOutput: "b"}},
		CallbackURL:    "https://example.com/webhook",
		CallbackSecret: secret,
	}
	evt := eventlog.New("job-enc", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-enc", set, evt, "ac:stream:enc"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	// Verify the raw Redis value is NOT the plaintext secret.
	raw, err := rdb.HGet(ctx, "ac:tests:job-enc", "callback_secret").Result()
	if err != nil {
		t.Fatalf("HGet callback_secret: %v", err)
	}
	if raw == secret {
		t.Error("callback_secret is stored as plaintext in Redis — expected AES-GCM ciphertext")
	}
	const sentinel = "enc:\x00"
	if len(raw) < len(sentinel) || raw[:len(sentinel)] != sentinel {
		t.Errorf("stored value does not have enc:\\x00 sentinel: %q", raw)
	}

	// GetTests must return the original plaintext secret.
	got, err := ps.GetTests(ctx, "job-enc")
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if got.CallbackSecret != secret {
		t.Errorf("CallbackSecret = %q, want %q", got.CallbackSecret, secret)
	}
	if got.CallbackURL != set.CallbackURL {
		t.Errorf("CallbackURL = %q, want %q", got.CallbackURL, set.CallbackURL)
	}
}

// TestCallbackSecret_BackwardsCompatibility verifies that GetTests returns a legacy
// plaintext secret (written before the master key was configured) unchanged.
func TestCallbackSecret_BackwardsCompatibility(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i + 7)
	}
	ps := NewPubSubWithKey(rdb, masterKey)

	// Simulate a pre-encryption Redis record (no enc: prefix).
	plaintextSecret := "old-plaintext-secret" //nolint:gosec // G101: test fixture, not a real credential
	if err := rdb.HSet(ctx, "ac:tests:job-legacy",
		"count", 1,
		"0:input", "x", "0:expected", "y",
		"callback_url", "https://example.com/cb",
		"callback_secret", plaintextSecret,
	).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	got, err := ps.GetTests(ctx, "job-legacy")
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if got.CallbackSecret != plaintextSecret {
		t.Errorf("CallbackSecret = %q, want %q (backwards compat)", got.CallbackSecret, plaintextSecret)
	}
}

// TestCallbackSecret_NoKeyNoEncryption verifies that when no master key is configured
// the secret is stored and returned verbatim (no-op / backwards-compat path).
func TestCallbackSecret_NoKeyNoEncryption(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	ps := NewPubSub(rdb) // no key

	secret := "plain-secret"
	set := TestSet{
		Tests:          []TestData{{Input: "1", ExpectedOutput: "2"}},
		CallbackSecret: secret,
	}
	evt := eventlog.New("job-nokey", eventlog.EventQueued, nil)
	if err := ps.PrepareAndEnqueue(ctx, "job-nokey", set, evt, "ac:stream:nokey"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	got, err := ps.GetTests(ctx, "job-nokey")
	if err != nil {
		t.Fatalf("GetTests: %v", err)
	}
	if got.CallbackSecret != secret {
		t.Errorf("CallbackSecret = %q, want %q", got.CallbackSecret, secret)
	}
}

// TestCallbackSecret_EncryptedWithoutKey verifies that GetTests returns an error
// when the stored secret carries the enc:\x00 sentinel but no master key is set.
func TestCallbackSecret_EncryptedWithoutKey(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	// Write an encrypted value directly using a key, then read it back without one.
	writeKey := make([]byte, 32)
	for i := range writeKey {
		writeKey[i] = byte(i + 3)
	}
	psWriter := NewPubSubWithKey(rdb, writeKey)
	set := TestSet{
		Tests:          []TestData{{Input: "a", ExpectedOutput: "b"}},
		CallbackSecret: "secret-value",
	}
	evt := eventlog.New("job-enc-nokey", eventlog.EventQueued, nil)
	if err := psWriter.PrepareAndEnqueue(ctx, "job-enc-nokey", set, evt, "ac:stream:enk"); err != nil {
		t.Fatalf("PrepareAndEnqueue: %v", err)
	}

	// Read back without a key — must error, not silently return garbage.
	psNoKey := NewPubSub(rdb)
	_, err := psNoKey.GetTests(ctx, "job-enc-nokey")
	if err == nil {
		t.Fatal("expected error reading encrypted secret without key, got nil")
	}
}
