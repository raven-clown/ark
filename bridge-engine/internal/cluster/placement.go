package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type placementRecord struct {
	Pipeline    string         `json:"pipeline"`
	GeneratedAt time.Time      `json:"generated_at"`
	Assignments map[string]int `json:"assignments"` // node_id -> workers
}

// placer is the leader-side producer of placement decisions and, on every
// node regardless of leadership, the tailer that keeps this process's view
// of the current cluster-wide placement up to date.
type placer struct {
	brokers []string
	writer  *producer.Producer
	log     *slog.Logger

	mu          sync.Mutex
	assignments map[string]map[string]int // pipeline -> node_id -> workers
}

func newPlacer(brokers []string, log *slog.Logger) *placer {
	return &placer{
		brokers:     brokers,
		writer:      producer.New(brokers, PlacementTopic),
		log:         log,
		assignments: make(map[string]map[string]int),
	}
}

// publish computes and writes a placement decision for every enabled
// pipeline, spreading each pipeline's configured Workers as evenly as
// possible across liveNodes. Only called by the current leader.
func (p *placer) publish(ctx context.Context, pipelines []config.Pipeline, liveNodes []string) error {
	nodes := make([]string, len(liveNodes))
	copy(nodes, liveNodes)
	sort.Strings(nodes)

	for _, pl := range pipelines {
		if !pl.IsEnabled() {
			continue
		}
		rec := placementRecord{
			Pipeline:    pl.Name,
			GeneratedAt: time.Now().UTC(),
			Assignments: distribute(pl.Workers, nodes),
		}
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := p.writer.Send(ctx, []byte(pl.Name), val, nil); err != nil {
			return err
		}
	}
	return nil
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

// watch tails PlacementTopic from the beginning and calls onUpdate with the
// full pipeline -> node_id -> workers view every time any record changes,
// for the lifetime of ctx. Every node runs this, independent of whether
// it's the leader, since every node needs to see the whole current
// placement to know its own share of it.
func (p *placer) watch(ctx context.Context, onUpdate func(map[string]map[string]int)) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     p.brokers,
		Topic:       PlacementTopic,
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
			p.log.Error("reading placements failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		var rec placementRecord
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			p.log.Error("decoding placement record failed", "error", err)
			continue
		}

		p.mu.Lock()
		p.assignments[rec.Pipeline] = rec.Assignments
		snapshot := make(map[string]map[string]int, len(p.assignments))
		for k, v := range p.assignments {
			snapshot[k] = v
		}
		p.mu.Unlock()

		onUpdate(snapshot)
	}
}
