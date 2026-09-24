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
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkatail"
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

// Pipelines returns the cluster's current pipeline config.
func (n *Node) Pipelines() []config.Pipeline { return n.distributedPipelines() }

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
func (n *Node) watchConfig(ctx context.Context) {
	var lastOffset int64 = -1
	kafkatail.Compacted(ctx, n.brokers, n.topics.Config, n.log,
		func(msg kafka.Message) {
			lastOffset = msg.Offset
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
			if n.configCaughtUp.Load() {
				n.applyDistributed(msg.Offset)
			}
		},
		func() {
			n.configCaughtUp.Store(true)
			if lastOffset >= 0 {
				n.applyDistributed(lastOffset)
			}
		})
}

func (n *Node) applyDistributed(version int64) {
	pipelines := n.distributedPipelines()
	if err := config.ValidatePipelines(pipelines); err != nil {
		n.log.Error("cluster pipeline config is invalid, keeping the last good one", "error", err, "version", version)
		return
	}
	if n.configVersion.Swap(version) != version {
		events.Record("", events.ConfigApplied, fmt.Sprintf("cluster pipeline config version %d applied on node %s (%d pipelines)", version, n.id, len(pipelines)), nil)
	}
	if n.onConfig != nil {
		n.onConfig(pipelines)
	}
}

// startConfig reads the config topic and, if the cluster has no config
// yet, seeds it from this node's local file. It returns once this node has
// applied a config, or after a bounded wait.
func (n *Node) startConfig(ctx context.Context, seed []config.Pipeline) error {
	go n.watchConfig(ctx)

	wait := func(done func() bool) error {
		deadline := time.Now().Add(placementCatchUpTimeout)
		for !done() && time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		return nil
	}

	if err := wait(n.configCaughtUp.Load); err != nil {
		return err
	}
	if n.configVersion.Load() < 0 {
		if err := n.PublishConfig(ctx, seed); err != nil {
			return fmt.Errorf("seeding pipeline config from the local file: %w", err)
		}
		n.log.Info("the cluster had no pipeline config, seeded it from the local file", "pipelines", len(seed))
	}
	return wait(func() bool { return n.configVersion.Load() >= 0 || len(seed) == 0 })
}
