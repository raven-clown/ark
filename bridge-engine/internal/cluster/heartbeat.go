package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type heartbeatRecord struct {
	NodeID   string    `json:"node_id"`
	LastSeen time.Time `json:"last_seen"`
}

// runHeartbeatProducer produces one heartbeat record for this node on every
// tick until ctx is done, then writes a tombstone for its key so the leader
// drops this node immediately instead of waiting out node_timeout.
func runHeartbeatProducer(ctx context.Context, w *producer.Producer, nodeID string, interval time.Duration, log *slog.Logger) {
	beat := func() {
		val, err := json.Marshal(heartbeatRecord{NodeID: nodeID, LastSeen: time.Now().UTC()})
		if err != nil {
			log.Error("marshaling heartbeat failed", "error", err)
			return
		}
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := w.Send(writeCtx, []byte(nodeID), val, nil); err != nil && ctx.Err() == nil {
			log.Error("producing heartbeat failed", "error", err)
		}
	}

	beat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			tombCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := w.Send(tombCtx, []byte(nodeID), nil, nil); err != nil {
				log.Warn("writing heartbeat tombstone on shutdown failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			beat()
		}
	}
}

// heartbeatView keeps this node's picture of which nodes are alive. A node
// counts as alive based on when this process last received its heartbeat,
// by this process's own clock, so clock skew between hosts can't make a
// live node look dead or a dead one look alive. Only heartbeats produced
// after this view started are counted; replaying old ones would say nothing
// about who is alive now.
type heartbeatView struct {
	startedAt time.Time

	mu   sync.RWMutex
	seen map[string]time.Time
}

func newHeartbeatView() *heartbeatView {
	return &heartbeatView{seen: make(map[string]time.Time), startedAt: time.Now()}
}

func (v *heartbeatView) run(ctx context.Context, brokers []string, topic string, log *slog.Logger) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("reading heartbeats failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		v.observe(string(msg.Key), msg.Value == nil, time.Now())
	}
}

func (v *heartbeatView) observe(nodeID string, tombstone bool, at time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if tombstone {
		delete(v.seen, nodeID)
		return
	}
	v.seen[nodeID] = at
}

// warm reports whether the view has been running long enough to have
// heard from every live node at least once.
func (v *heartbeatView) warm(heartbeatInterval time.Duration) bool {
	return time.Since(v.startedAt) >= 2*heartbeatInterval
}

// liveNodes returns the IDs of every node heard from within timeout, and
// forgets nodes silent for much longer than that so the map can't grow
// without bound across restarts.
func (v *heartbeatView) liveNodes(timeout time.Duration) []string {
	now := time.Now()
	cutoff := now.Add(-timeout)
	forget := now.Add(-10 * timeout)

	v.mu.Lock()
	defer v.mu.Unlock()

	out := make([]string, 0, len(v.seen))
	for id, lastSeen := range v.seen {
		switch {
		case lastSeen.After(cutoff):
			out = append(out, id)
		case lastSeen.Before(forget):
			delete(v.seen, id)
		}
	}
	return out
}
