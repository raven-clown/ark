package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type placementRecord struct {
	Pipeline    string         `json:"pipeline"`
	Epoch       int64          `json:"epoch"`
	GeneratedAt time.Time      `json:"generated_at"`
	Assignments map[string]int `json:"assignments"` // node_id -> workers
}

// placer is the leader-side producer of placement decisions and, on every
// node, the tailer that keeps this process's view of the cluster-wide
// placement current.
type placer struct {
	brokers []string
	topic   string
	writer  *producer.Producer
	log     *slog.Logger

	caughtUp atomic.Bool

	mu          sync.Mutex
	assignments map[string]map[string]int // pipeline -> node_id -> workers
	maxEpoch    int64

	// leader-side memory of what this node last published, so unchanged
	// placements aren't rewritten every tick.
	pubEpoch int64
	pub      map[string]map[string]int
}

func newPlacer(brokers []string, topic string, log *slog.Logger) *placer {
	return &placer{
		brokers:     brokers,
		topic:       topic,
		writer:      producer.New(brokers, topic),
		log:         log,
		assignments: make(map[string]map[string]int),
		pub:         make(map[string]map[string]int),
	}
}

// leaderEpoch returns the epoch a new leader publishes under: never lower
// than anything already in the topic, so every node prefers the new
// leader's records over a deposed leader's late writes. It waits until this
// node has read the whole placement topic, otherwise it couldn't know the
// highest epoch in it.
func (p *placer) leaderEpoch(ctx context.Context, generation int32) (int64, error) {
	for !p.caughtUp.Load() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return max(int64(generation), p.maxEpoch+1), nil
}

// publish writes, in one batch, the placement for every enabled pipeline
// whose computed assignment differs from what this leader last published
// under epoch, plus a tombstone for any pipeline no longer configured. It
// returns how many records it wrote.
func (p *placer) publish(ctx context.Context, epoch int64, pipelines []config.Pipeline, liveNodes []string) (int, error) {
	nodes := make([]string, len(liveNodes))
	copy(nodes, liveNodes)
	sort.Strings(nodes)

	p.mu.Lock()
	if p.pubEpoch != epoch {
		p.pubEpoch = epoch
		p.pub = make(map[string]map[string]int)
	}
	prev := maps.Clone(p.pub)
	p.mu.Unlock()

	now := time.Now().UTC()
	next := make(map[string]map[string]int, len(pipelines))
	var msgs []kafka.Message
	for _, pl := range pipelines {
		if !pl.IsEnabled() {
			continue
		}
		assign := distribute(pl.Workers, nodes)
		next[pl.Name] = assign
		if old, ok := prev[pl.Name]; ok && maps.Equal(old, assign) {
			continue
		}
		val, err := json.Marshal(placementRecord{Pipeline: pl.Name, Epoch: epoch, GeneratedAt: now, Assignments: assign})
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, kafka.Message{Key: []byte(pl.Name), Value: val})
	}
	for name := range prev {
		if _, ok := next[name]; !ok {
			msgs = append(msgs, kafka.Message{Key: []byte(name), Value: nil})
		}
	}

	if len(msgs) == 0 {
		return 0, nil
	}
	if err := p.writer.SendMany(ctx, msgs...); err != nil {
		return 0, err
	}

	p.mu.Lock()
	if p.pubEpoch == epoch {
		p.pub = next
	}
	p.mu.Unlock()
	return len(msgs), nil
}

// distribute spreads total worker slots across nodes as evenly as
// possible: base = total/len(nodes) each, with the remainder handed one
// each to the first `total%len(nodes)` nodes in sorted order.
func distribute(total int, nodes []string) map[string]int {
	out := make(map[string]int, len(nodes))
	if len(nodes) == 0 || total <= 0 {
		return out
	}
	base := total / len(nodes)
	rem := total % len(nodes)
	for i, id := range nodes {
		w := base
		if i < rem {
			w++
		}
		out[id] = w
	}
	return out
}

func (p *placer) highWatermark(ctx context.Context) (int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", p.brokers[0], p.topic, 0)
	if err != nil {
		return 0, fmt.Errorf("dialing leader for %s: %w", p.topic, err)
	}
	defer conn.Close()
	return conn.ReadLastOffset()
}

// watch tails the placement topic from the beginning. Records from an epoch
// older than the newest one already seen are ignored: they come from a
// leader that has since been replaced. onUpdate receives the full
// pipeline -> node_id -> workers view, but only once the replay of existing
// records is complete, so a restarting node reconciles once against the
// current placement rather than once per historical record.
func (p *placer) watch(ctx context.Context, onUpdate func(map[string]map[string]int)) {
	var hw int64
	for {
		var err error
		hw, err = p.highWatermark(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		p.log.Error("reading placement topic end offset failed", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}

	notify := func() {
		p.mu.Lock()
		snapshot := make(map[string]map[string]int, len(p.assignments))
		for k, v := range p.assignments {
			snapshot[k] = v
		}
		p.mu.Unlock()
		onUpdate(snapshot)
	}

	if hw == 0 {
		p.caughtUp.Store(true)
		notify()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     p.brokers,
		Topic:       p.topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("reading placements failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		p.apply(msg)

		if !p.caughtUp.Load() {
			if msg.Offset+1 < hw {
				continue
			}
			p.caughtUp.Store(true)
		}
		notify()
	}
}

func (p *placer) apply(msg kafka.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if msg.Value == nil {
		delete(p.assignments, string(msg.Key))
		return
	}
	var rec placementRecord
	if err := json.Unmarshal(msg.Value, &rec); err != nil {
		p.log.Error("decoding placement record failed", "error", err)
		return
	}
	if rec.Epoch < p.maxEpoch {
		p.log.Warn("ignoring placement from a deposed leader", "pipeline", rec.Pipeline, "epoch", rec.Epoch, "current_epoch", p.maxEpoch)
		return
	}
	p.maxEpoch = rec.Epoch
	p.assignments[rec.Pipeline] = rec.Assignments
}
