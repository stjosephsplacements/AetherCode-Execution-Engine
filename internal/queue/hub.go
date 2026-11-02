package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
)

const (
	eventChannelPrefix = "ac:events:"
	// subscriberBuffer is how many events one SSE client may be behind before
	// it is cut loose to reconnect and replay from the event log.
	subscriberBuffer = 16
)

// hub fans one Redis pattern subscription out to every SSE client of this
// process. Before it, each client held its own Redis subscription, so 5,000
// open streams meant 5,000 Redis connections.
type hub struct {
	mu     sync.Mutex
	subs   map[string]map[chan eventlog.Event]struct{}
	cancel context.CancelFunc
	done   chan struct{} // closed when the hub stops, after every client channel is closed
}

// startHub opens the shared subscription and waits for Redis to confirm it, so
// a client that subscribes after this returns cannot miss a later event.
func (p *PubSub) startHub() (*hub, error) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := p.rdb.PSubscribe(ctx, eventChannelPrefix+"*")
	if _, err := sub.Receive(ctx); err != nil {
		cancel()
		_ = sub.Close()
		return nil, err
	}
	h := &hub{subs: make(map[string]map[chan eventlog.Event]struct{}), cancel: cancel, done: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer h.closeAll()
		defer sub.Close() //nolint:errcheck // best-effort: the connection is going away
		messages := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-messages:
				if !ok {
					return
				}
				h.dispatch(msg)
			}
		}
	}()
	return h, nil
}

// getHub returns the running hub, starting it on first use. A failed start is
// retried by the next caller.
func (p *PubSub) getHub() (*hub, error) {
	p.hubMu.Lock()
	defer p.hubMu.Unlock()
	if p.hub != nil {
		return p.hub, nil
	}
	h, err := p.startHub()
	if err != nil {
		return nil, err
	}
	p.hub = h
	return h, nil
}

// stopHub ends the shared subscription. Wait calls it before waiting.
func (p *PubSub) stopHub() {
	p.hubMu.Lock()
	defer p.hubMu.Unlock()
	if p.hub != nil {
		p.hub.cancel()
	}
}

func (h *hub) add(jobID string, ch chan eventlog.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[jobID] == nil {
		h.subs[jobID] = make(map[chan eventlog.Event]struct{})
	}
	h.subs[jobID][ch] = struct{}{}
}

// remove unregisters a client. It reports whether the client was still
// registered, so the caller closes its channel only if the hub has not already.
func (h *hub) remove(jobID string, ch chan eventlog.Event) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[jobID][ch]; !ok {
		return false
	}
	delete(h.subs[jobID], ch)
	if len(h.subs[jobID]) == 0 {
		delete(h.subs, jobID)
	}
	return true
}

// dispatch hands a message to the clients of its job. A client whose buffer is
// full is closed instead of skipped: the SSE handler turns a closed channel
// into a reconnect, and the reconnect replays every event from the log.
func (h *hub) dispatch(msg *redis.Message) {
	jobID, ok := strings.CutPrefix(msg.Channel, eventChannelPrefix)
	if !ok || strings.Contains(jobID, ":") {
		return
	}
	h.mu.Lock()
	if len(h.subs[jobID]) == 0 {
		h.mu.Unlock()
		return
	}
	var evt eventlog.Event
	if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
		h.mu.Unlock()
		return
	}
	for ch := range h.subs[jobID] {
		select {
		case ch <- evt:
		default:
			slog.Warn("sse: client fell behind, closing it to reconnect", "job_id", jobID)
			delete(h.subs[jobID], ch)
			close(ch)
		}
	}
	if len(h.subs[jobID]) == 0 {
		delete(h.subs, jobID)
	}
	h.mu.Unlock()
}

func (h *hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer close(h.done)
	for jobID, set := range h.subs {
		for ch := range set {
			close(ch)
		}
		delete(h.subs, jobID)
	}
}
