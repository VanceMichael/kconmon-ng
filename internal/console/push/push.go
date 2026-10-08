package push

// Package push owns the snapshot pub/sub abstraction (ADR-003): snapshot topics
// are LOCAL-ONLY. snapshot topics never touch a bus.

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	events "github.com/EsDmitrii/kconmon-ng/internal/console/events"

	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/ws"
)

// nudgeDedupeCap bounds the relay's seen-id set; the same bounded LRU the hub uses for frames.
const nudgeDedupeCap = 512

// Nudger is something that can be asked to recompute right now.
type Nudger interface{ Nudge() }

// signalNudge is non-blocking coalescing put into a channel with capacity 1 and
// "default" on write. Multiple topology changes inside the interval coalesce
// into a single extra recomputation, exactly like a pub/sub SET key expiry fan
// on a crowded set.
func signalNudge(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// nudgeSet dedupes topology_changed ids, so a replay or a multi-replica duplicate — including an
// unconfirmed→confirmed upgrade — nudges the derived snapshot exactly once per change.
type nudgeSet struct {
	mu    sync.Mutex
	order []string
	seen  map[string]struct{}
}

func (s *nudgeSet) add(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[id]; ok {
		return false
	}
	s.seen[id] = struct{}{}
	s.order = append(s.order, id)
	if len(s.order) > nudgeDedupeCap {
		delete(s.seen, s.order[0])
		s.order = s.order[1:]
	}
	return true
}

// RunNudgeRelay subscribes to "live" and asks the nudgers to recompute once per CONFIRMED
// topology_changed change. Unconfirmed frames (a storage outage) never drive derived state: the
// change they describe is not history yet, and the pusher's own interval tick covers the delay.
func RunNudgeRelay(ctx context.Context, bus cache.Bus, nudgers ...Nudger) {
	msgs, unsubscribe := bus.Subscribe("live")
	defer unsubscribe()

	deduped := &nudgeSet{seen: make(map[string]struct{}, nudgeDedupeCap)}
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				// Defensive; unsubscribe (the only manager closes a subscription while this loop
				// reads) cannot happen while this loop is still reading.
				return
			}
			if msg.Type == ws.TypeEventPending {
				continue
			}
			var ev events.LiveEvent
			if err := json.Unmarshal(msg.Data, &ev); err != nil {
				slog.Warn("nudge relay: dropping malformed live event", "error", err)
				continue
			}
			if ev.Type != events.TypeTopologyChanged {
				continue
			}
			if !deduped.add(ev.ID) {
				continue
			}
			for _, n := range nudgers {
				n.Nudge()
			}
		}
	}
}
