package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// Pipeline config in cluster mode lives in a compacted topic keyed by
// pipeline name, so every node runs the same config no matter which node's
// file was edited. The local YAML only seeds an empty topic; after that, a
// reload on any node publishes its file's pipelines here, and every node
// (that one included) applies what it reads back.

// SetConfigHandler sets what applies the cluster-wide pipeline config on
// this node. Call before Start.
func (n *Node) SetConfigHandler(fn func(pipelines []config.Pipeline)) { n.onConfig = fn }

// ConfigVersion is the offset of the last config record this node applied.
func (n *Node) ConfigVersion() int64 { return n.configVersion.Load() }

func (n *Node) distributedPipelines() []config.Pipeline {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	out := make([]config.Pipeline, 0, len(n.distributed))
	for _, p := range n.distributed {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// PublishConfig makes pipelines the cluster's config: changed or new
// pipelines are written, pipelines no longer present are tombstoned, all in
// one batch. It validates first, so a bad file never reaches other nodes.
func (n *Node) PublishConfig(ctx context.Context, pipelines []config.Pipeline) error {
	if err := config.ValidatePipelines(pipelines); err != nil {
		return err
	}

	current := make(map[string]config.Pipeline)
	for _, p := range n.distributedPipelines() {
		current[p.Name] = p
	}

	var msgs []kafka.Message
	seen := make(map[string]bool, len(pipelines))
	for _, p := range pipelines {
		seen[p.Name] = true
		if old, ok := current[p.Name]; ok && reflect.DeepEqual(old, p) {
			continue
		}
		val, err := json.Marshal(p)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{Key: []byte(p.Name), Value: val})
	}
	for name := range current {
		if !seen[name] {
			msgs = append(msgs, kafka.Message{Key: []byte(name), Value: nil})
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	if err := n.configWriter.SendMany(ctx, msgs...); err != nil {
		return fmt.Errorf("publishing pipeline config: %w", err)
	}
	n.log.Info("published pipeline config to the cluster", "records", len(msgs))
	return nil
}

// watchConfig tails the config topic and applies the full config once the
// existing records have been read, then again after every change. A config
// that fails validation is logged and skipped; nodes keep the last good one.
func (n *Node) watchConfig(ctx context.Context, hw int64) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     n.brokers,
		Topic:       n.topics.Config,
		Partition:   0,
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
			n.log.Error("reading pipeline config topic failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		n.cfgMu.Lock()
		if msg.Value == nil {
			delete(n.distributed, string(msg.Key))
		} else {
			var p config.Pipeline
			if err := json.Unmarshal(msg.Value, &p); err != nil {
				n.log.Error("decoding pipeline config record failed", "pipeline", string(msg.Key), "error", err)
			} else {
				n.distributed[p.Name] = p
			}
		}
		n.cfgMu.Unlock()

		if !n.configCaughtUp.Load() {
			if msg.Offset+1 < hw {
				continue
			}
			n.configCaughtUp.Store(true)
		}
		n.applyDistributed(msg.Offset)
	}
}

func (n *Node) applyDistributed(version int64) {
	pipelines := n.distributedPipelines()
	if err := config.ValidatePipelines(pipelines); err != nil {
		n.log.Error("cluster pipeline config is invalid, keeping the last good one", "error", err, "version", version)
		return
	}
	n.configVersion.Store(version)
	if n.onConfig != nil {
		n.onConfig(pipelines)
	}
}

// startConfig reads the config topic and, if it's empty, seeds it from
// this node's local file. It returns once this node has applied a config
// (or the topic was just seeded and read back).
func (n *Node) startConfig(ctx context.Context, seed []config.Pipeline) error {
	hw, err := n.endOffset(ctx, n.topics.Config)
	if err != nil {
		return err
	}
	if hw == 0 {
		n.configCaughtUp.Store(true)
	}
	go n.watchConfig(ctx, hw)

	if hw == 0 {
		if err := n.PublishConfig(ctx, seed); err != nil {
			return fmt.Errorf("seeding pipeline config from the local file: %w", err)
		}
		n.log.Info("pipeline config topic was empty, seeded it from the local file", "pipelines", len(seed))
	}

	deadline := time.Now().Add(placementCatchUpTimeout)
	for n.configVersion.Load() < 0 && len(seed) > 0 && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}

func (n *Node) endOffset(ctx context.Context, topic string) (int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", n.brokers[0], topic, 0)
	if err != nil {
		return 0, fmt.Errorf("dialing leader for %s: %w", topic, err)
	}
	defer func() { _ = conn.Close() }()
	return conn.ReadLastOffset()
}
