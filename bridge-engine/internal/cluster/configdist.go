package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkatail"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

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

func (n *Node) watchConfig(ctx context.Context) {
	var lastOffset int64 = -1
	kafkatail.Compacted(ctx, n.brokers, n.topics.Config, n.log,
		func(msg kafka.Message) {
			lastOffset = msg.Offset
			n.cfgMu.Lock()
			if name, ok := strings.CutPrefix(string(msg.Key), projectKeyPrefix); ok {
				if msg.Value == nil {
					delete(n.projects, name)
				} else {
					var p config.Project
					if err := json.Unmarshal(msg.Value, &p); err != nil {
						n.log.Error("decoding project config record failed", "project", name, "error", err)
					} else {
						n.projects[p.Name] = p
					}
				}
			} else if msg.Value == nil {
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

func (n *Node) startConfig(ctx context.Context, seed []config.Pipeline) error {
	go n.watchConfig(ctx)

	wait := func(done func() bool) error {
		deadline := time.Now().Add(tuning.ClusterCatchUp())
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
		if len(n.seedProjects) > 0 {
			if err := n.PublishProjects(ctx, n.seedProjects); err != nil {
				return fmt.Errorf("seeding project config from the local file: %w", err)
			}
		}
		if err := n.PublishConfig(ctx, seed); err != nil {
			return fmt.Errorf("seeding pipeline config from the local file: %w", err)
		}
		n.log.Info("the cluster had no pipeline config, seeded it from the local file", "pipelines", len(seed))
	}
	return wait(func() bool { return n.configVersion.Load() >= 0 || len(seed) == 0 })
}

const projectKeyPrefix = "@project:"

func (n *Node) SetSeedProjects(projects []config.Project) { n.seedProjects = projects }

// Projects returns the cluster's current projects.
func (n *Node) Projects() []config.Project {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	out := make([]config.Project, 0, len(n.projects))
	for _, p := range n.projects {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (n *Node) PublishProjects(ctx context.Context, projects []config.Project) error {
	if err := config.ValidateProjects(projects, n.distributedPipelines()); err != nil {
		return err
	}
	current := map[string]config.Project{}
	for _, p := range n.Projects() {
		current[p.Name] = p
	}
	var msgs []kafka.Message
	seen := map[string]bool{}
	for _, p := range projects {
		seen[p.Name] = true
		if old, ok := current[p.Name]; ok && reflect.DeepEqual(old, p) {
			continue
		}
		val, err := json.Marshal(p)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{Key: []byte(projectKeyPrefix + p.Name), Value: val})
	}
	for name := range current {
		if !seen[name] {
			msgs = append(msgs, kafka.Message{Key: []byte(projectKeyPrefix + name), Value: nil})
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	if err := n.configWriter.SendMany(ctx, msgs...); err != nil {
		return fmt.Errorf("publishing project config: %w", err)
	}
	return nil
}
