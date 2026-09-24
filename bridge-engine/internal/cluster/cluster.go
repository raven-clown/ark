// Package cluster implements ARK Cluster (PLAN.md, Phase 4b/4c): an opt-in
// mechanism for spreading a pipeline's workers across multiple ARK
// processes, using Kafka itself as the coordination backbone instead of an
// embedded Raft implementation or an external coordinator.
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

// Topics are namespaced by cluster.name so separate ARK deployments sharing
// one Kafka cluster never join each other's election or read each other's
// placements.
type Topics struct {
	Heartbeat string
	Election  string
	Placement string
}

func TopicsFor(clusterName string) Topics {
	prefix := "__ark_" + clusterName + "_"
	return Topics{
		Heartbeat: prefix + "cluster_nodes",
		Election:  prefix + "leader_election",
		Placement: prefix + "placements",
	}
}

// ReconcileFunc applies a set of pipeline configs to the local orchestrator,
// with the same contract as orchestrator.Manager.Reconcile.
type ReconcileFunc func(pipelines []config.Pipeline) []error

type Status struct {
	NodeID    string   `json:"node_id"`
	Cluster   string   `json:"cluster"`
	Leader    bool     `json:"leader"`
	LiveNodes []string `json:"live_nodes"`
}

// Node coordinates this process's participation in an ARK cluster: it
// heartbeats its own liveness, takes part in leader election, and, whether
// or not it currently is leader, applies whatever placement decision the
// current leader has published for pipelines it should run locally.
type Node struct {
	id                string
	brokers           []string
	cfg               config.Cluster
	topics            Topics
	replicationFactor int
	reconcile         ReconcileFunc
	log               *slog.Logger

	hbView  *heartbeatView
	elector *elector
	placer  *placer

	mu         sync.Mutex
	base       []config.Pipeline // last config seen via ApplyConfig
	configured bool
	local      map[string]int // pipeline name -> workers assigned to this node; nil until the first placement view arrives
}

func New(brokers []string, cfg config.Cluster, replicationFactor int, reconcile ReconcileFunc, log *slog.Logger) *Node {
	id := cfg.NodeID
	if id == "" {
		id = defaultNodeID()
	}
	log = log.With("component", "cluster", "cluster", cfg.Name, "node_id", id)

	return &Node{
		id:                id,
		brokers:           brokers,
		cfg:               cfg,
		topics:            TopicsFor(cfg.Name),
		replicationFactor: replicationFactor,
		reconcile:         reconcile,
		log:               log,
		hbView:            newHeartbeatView(),
	}
}

func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "ark-node"
	}
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return host
	}
	return fmt.Sprintf("%s-%s", host, hex.EncodeToString(buf))
}

func (n *Node) ID() string { return n.id }

// Start ensures the internal cluster topics exist and launches every
// background loop (heartbeat, election, placement). It returns once setup
// succeeds; the loops keep running until ctx is cancelled.
func (n *Node) Start(ctx context.Context) error {
	if err := kafkaadmin.EnsureCompactedTopic(ctx, n.brokers, n.topics.Heartbeat, 1, n.replicationFactor); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", n.topics.Heartbeat, err)
	}
	if err := kafkaadmin.EnsureTopic(ctx, n.brokers, n.topics.Election, 1, n.replicationFactor); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", n.topics.Election, err)
	}
	if err := kafkaadmin.EnsureCompactedTopic(ctx, n.brokers, n.topics.Placement, 1, n.replicationFactor); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", n.topics.Placement, err)
	}

	go n.hbView.run(ctx, n.brokers, n.topics.Heartbeat, n.log)
	go runHeartbeatProducer(ctx, producer.New(n.brokers, n.topics.Heartbeat), n.id, n.heartbeatInterval(), n.log) // #nosec G118 -- the shutdown tombstone must be written after ctx is cancelled

	n.placer = newPlacer(n.brokers, n.topics.Placement, n.log)
	go n.placer.watch(ctx, n.onPlacementUpdate)

	// Give the first ApplyConfig the current placement to work from, so a
	// joining node starts with its real share instead of starting at full
	// workers and being narrowed a moment later (two restarts per join).
	deadline := time.Now().Add(placementCatchUpTimeout)
	for !n.placer.caughtUp.Load() && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	el, err := newElector(n.brokers, n.topics.Election, n.nodeTimeout(), n.log)
	if err != nil {
		return fmt.Errorf("starting leader election: %w", err)
	}
	n.elector = el
	go el.run(ctx, n.runLeaderDuties)

	return nil
}

const placementCatchUpTimeout = 10 * time.Second

func (n *Node) heartbeatInterval() time.Duration {
	return time.Duration(n.cfg.HeartbeatIntervalSeconds) * time.Second
}

func (n *Node) nodeTimeout() time.Duration {
	return time.Duration(n.cfg.NodeTimeoutSeconds) * time.Second
}

// runLeaderDuties runs for as long as this node holds one election
// generation. It publishes only placements that changed since its last
// publish, under an epoch newer than any earlier leader's.
func (n *Node) runLeaderDuties(genCtx context.Context, generation int32) {
	epoch, err := n.placer.leaderEpoch(genCtx, generation)
	if err != nil {
		return
	}
	n.log.Info("became cluster leader", "epoch", epoch)
	defer n.log.Info("lost cluster leadership", "epoch", epoch)

	ticker := time.NewTicker(time.Duration(n.cfg.PlacementIntervalSeconds) * time.Second)
	defer ticker.Stop()

	publish := func() {
		if !n.hbView.warm(n.heartbeatInterval()) {
			return // not every live node has been heard from yet
		}
		n.mu.Lock()
		pipelines := n.base
		n.mu.Unlock()

		live := n.hbView.liveNodes(n.nodeTimeout())
		if len(live) == 0 {
			live = []string{n.id}
		}
		written, err := n.placer.publish(genCtx, epoch, pipelines, live)
		if err != nil {
			if genCtx.Err() == nil {
				n.log.Error("publishing placement failed", "error", err)
			}
			return
		}
		if written > 0 {
			n.log.Info("published placement", "records", written, "live_nodes", len(live), "epoch", epoch)
		}
	}

	publish()
	for {
		select {
		case <-genCtx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}

// ApplyConfig is called with the pipeline set every time the local config
// (re)loads. It stores the pipelines as the cluster-wide desired state and
// reconciles this node's local runners against the latest known placement.
func (n *Node) ApplyConfig(pipelines []config.Pipeline) []error {
	n.mu.Lock()
	n.base = pipelines
	n.configured = true
	n.mu.Unlock()
	return n.reconcileLocal(pipelines)
}

func (n *Node) onPlacementUpdate(assignments map[string]map[string]int) {
	local := make(map[string]int, len(assignments))
	for pipeline, byNode := range assignments {
		// A pipeline the leader has placed but not on this node gets 0
		// here, rather than falling back to its full configured workers.
		local[pipeline] = byNode[n.id]
	}

	n.mu.Lock()
	n.local = local
	pipelines := n.base
	configured := n.configured
	n.mu.Unlock()

	if !configured {
		return // ApplyConfig hasn't run yet; it will pick up this placement
	}
	for _, err := range n.reconcileLocal(pipelines) {
		n.log.Error("applying placement failed to start a pipeline", "error", err)
	}
}

// reconcileLocal narrows each pipeline's Workers to this node's share of the
// current placement decision, then reconciles.
//
// A pipeline the leader hasn't decided on yet keeps its full configured
// Workers, the same as single-node mode: over-provisioning a few idle
// consumers before placement stabilizes is harmless, since Kafka's own
// group coordinator still only ever hands each partition to one consumer,
// while under-provisioning would leave the pipeline unprocessed.
func (n *Node) reconcileLocal(pipelines []config.Pipeline) []error {
	n.mu.Lock()
	local := n.local
	n.mu.Unlock()

	if local == nil {
		return n.reconcile(pipelines)
	}

	adjusted := make([]config.Pipeline, 0, len(pipelines))
	for _, p := range pipelines {
		if w, ok := local[p.Name]; ok {
			if w <= 0 {
				continue
			}
			p.Workers = w
		}
		adjusted = append(adjusted, p)
	}
	return n.reconcile(adjusted)
}

// AssignedElsewhere reports whether name is a configured pipeline that the
// leader placed entirely on other nodes, so the API can say so instead of
// answering "not found".
func (n *Node) AssignedElsewhere(name string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	w, ok := n.local[name]
	if !ok || w > 0 {
		return false
	}
	for _, p := range n.base {
		if p.Name == name {
			return true
		}
	}
	return false
}

func (n *Node) StatusSnapshot() Status {
	live := n.hbView.liveNodes(n.nodeTimeout())
	sort.Strings(live)
	leader := n.elector != nil && n.elector.IsLeader()
	return Status{NodeID: n.id, Cluster: n.cfg.Name, Leader: leader, LiveNodes: live}
}
