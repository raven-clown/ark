package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkatail"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type placementRecord struct {
	Pipeline    string         `json:"pipeline"`
	Epoch       int64          `json:"epoch"`
	GeneratedAt time.Time      `json:"generated_at"`
	Assignments map[string]int `json:"assignments"` // node_id -> workers
}

type placer struct {
	brokers []string
	topic   string
	writer  *producer.Producer
	log     *slog.Logger

	caughtUp atomic.Bool

	mu          sync.Mutex
	assignments map[string]map[string]int // pipeline -> node_id -> workers
	maxEpoch    int64

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

// eligible returns, sorted, the live nodes whose labels satisfy selector.
func eligible(live map[string]heartbeatRecord, selector map[string]string) []string {
	out := make([]string, 0, len(live))
	for id, rec := range live {
		ok := true
		for k, v := range selector {
			if rec.Labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (p *placer) publish(ctx context.Context, epoch int64, pipelines []config.Pipeline, live map[string]heartbeatRecord, partitions map[string]int) (int, error) {
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
		total := pl.Workers
		if n := partitions[pl.SourceTopic]; n > 0 {
			total = min(total, n)
		}
		assign := distribute(total, eligible(live, pl.Placement.NodeSelector))
		if len(assign) == 0 {
			p.log.Warn("no live node matches this pipeline's node_selector, it runs nowhere until one joins", "pipeline", pl.Name, "node_selector", pl.Placement.NodeSelector)
		}
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

func (p *placer) watch(ctx context.Context, onUpdate func(map[string]map[string]int)) {
	notify := func() {
		p.mu.Lock()
		snapshot := make(map[string]map[string]int, len(p.assignments))
		for k, v := range p.assignments {
			snapshot[k] = v
		}
		p.mu.Unlock()
		onUpdate(snapshot)
	}
	kafkatail.Compacted(ctx, p.brokers, p.topic, p.log,
		func(msg kafka.Message) {
			p.apply(msg)
			if p.caughtUp.Load() {
				notify()
			}
		},
		func() {
			p.caughtUp.Store(true)
			notify()
		})
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
