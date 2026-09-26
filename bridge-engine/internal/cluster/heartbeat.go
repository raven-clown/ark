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

// PipelineStats is one node's view of one pipeline, carried in its
// heartbeat so any node can answer for the whole cluster.
type PipelineStats struct {
	Workers       int     `json:"workers"`
	Running       int     `json:"running"`
	Processed     int64   `json:"processed"`
	Rejected      int64   `json:"rejected"`
	DeadLettered  int64   `json:"dead_lettered"`
	Failed        int64   `json:"failed"`
	Paused        bool    `json:"paused"`
	Lag           int64   `json:"lag"`
	CallbackCalls int64   `json:"callback_calls"`
	AvgCallbackMs float64 `json:"avg_callback_ms"`
	BreakerOpen   bool    `json:"breaker_open,omitempty"`
}

type heartbeatRecord struct {
	NodeID    string                   `json:"node_id"`
	LastSeen  time.Time                `json:"last_seen"`
	Labels    map[string]string        `json:"labels,omitempty"`
	Leader    bool                     `json:"leader,omitempty"`
	Pipelines map[string]PipelineStats `json:"pipelines,omitempty"`
	// ConfigVersion lets any node see which nodes haven't applied the
	// latest cluster config yet.
	ConfigVersion int64 `json:"config_version"`
}

// runHeartbeatProducer produces one heartbeat record for this node on every
// tick until ctx is done, then writes a tombstone for its key so the leader
// drops this node immediately instead of waiting out node_timeout.
func runHeartbeatProducer(ctx context.Context, w *producer.Producer, nodeID string, interval time.Duration, snapshot func() heartbeatRecord, log *slog.Logger) {
	beat := func() {
		rec := snapshot()
		rec.NodeID, rec.LastSeen = nodeID, time.Now().UTC()
		val, err := json.Marshal(rec)
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
	info map[string]heartbeatRecord
}

func newHeartbeatView() *heartbeatView {
	return &heartbeatView{seen: make(map[string]time.Time), info: make(map[string]heartbeatRecord), startedAt: time.Now()}
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
		var rec heartbeatRecord
		if msg.Value != nil {
			if err := json.Unmarshal(msg.Value, &rec); err != nil {
				log.Error("decoding heartbeat failed", "error", err)
				continue
			}
		}
		v.observe(string(msg.Key), msg.Value == nil, time.Now(), rec)
	}
}

func (v *heartbeatView) observe(nodeID string, tombstone bool, at time.Time, rec heartbeatRecord) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if tombstone {
		delete(v.seen, nodeID)
		delete(v.info, nodeID)
		return
	}
	v.seen[nodeID] = at
	v.info[nodeID] = rec
}

// liveInfo returns the last heartbeat of every live node.
func (v *heartbeatView) liveInfo(timeout time.Duration) map[string]heartbeatRecord {
	live := v.liveNodes(timeout)
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make(map[string]heartbeatRecord, len(live))
	for _, id := range live {
		out[id] = v.info[id]
	}
	return out
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
			delete(v.info, id)
		}
	}
	return out
}
