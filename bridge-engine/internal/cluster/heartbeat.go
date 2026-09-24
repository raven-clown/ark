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

// runHeartbeatProducer produces one heartbeat record for this node to
// HeartbeatTopic on every tick, keyed by node ID, until ctx is done.
func runHeartbeatProducer(ctx context.Context, w *producer.Producer, nodeID string, interval time.Duration, log *slog.Logger) {
	beat := func() {
		rec := heartbeatRecord{NodeID: nodeID, LastSeen: time.Now().UTC()}
		val, err := json.Marshal(rec)
		if err != nil {
			log.Error("marshaling heartbeat failed", "error", err)
			return
		}
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := w.Send(writeCtx, []byte(nodeID), val, nil); err != nil {
			log.Error("producing heartbeat failed", "error", err)
		}
	}

	beat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			beat()
		}
	}
}

// heartbeatView keeps this node's independent, continuously-updated picture
// of every node's last heartbeat, read directly off HeartbeatTopic rather
// than through a shared consumer group, since every node needs to see the
// whole picture rather than a partition slice of it.
type heartbeatView struct {
	mu   sync.RWMutex
	seen map[string]time.Time
}

func newHeartbeatView() *heartbeatView {
	return &heartbeatView{seen: make(map[string]time.Time)}
}

func (v *heartbeatView) run(ctx context.Context, brokers []string, log *slog.Logger) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       HeartbeatTopic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     time.Second,
		StartOffset: kafka.FirstOffset,
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

		var rec heartbeatRecord
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			log.Error("decoding heartbeat record failed", "error", err)
			continue
		}

		v.mu.Lock()
		v.seen[rec.NodeID] = rec.LastSeen
		v.mu.Unlock()
	}
}

// liveNodes returns the IDs of every node whose last heartbeat is within
// timeout of now, sorted for deterministic placement decisions.
func (v *heartbeatView) liveNodes(timeout time.Duration) []string {
	cutoff := time.Now().Add(-timeout)

	v.mu.RLock()
	defer v.mu.RUnlock()

	out := make([]string, 0, len(v.seen))
	for id, lastSeen := range v.seen {
		if lastSeen.After(cutoff) {
			out = append(out, id)
		}
	}
	return out
}
