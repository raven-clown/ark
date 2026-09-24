// Package cluster implements ARK Cluster (PLAN.md, Phase 4b): an opt-in
// mechanism for spreading a pipeline's workers across multiple ARK
// processes, using Kafka itself as the coordination backbone instead of an
// embedded Raft implementation or an external coordinator. See PLAN.md for
// the full design and the reasoning behind that choice.
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

const (
	HeartbeatTopic = "__ark_cluster_nodes"
	ElectionTopic  = "__ark_leader_election"
	PlacementTopic = "__ark_placements"
)

// ReconcileFunc applies a set of pipeline configs to the local orchestrator,
// with the same contract as orchestrator.Manager.Reconcile.
type ReconcileFunc func(pipelines []config.Pipeline) []error

type Status struct {
	NodeID    string   `json:"node_id"`
	Leader    bool     `json:"leader"`
	LiveNodes []string `json:"live_nodes"`
}

// Node coordinates this process's participation in an ARK cluster: it
// heartbeats its own liveness, takes part in leader election, and, whether
// or not it currently is leader, applies whatever placement decision the
// current leader has published for pipelines it should run locally.
type Node struct {
	id        string
	brokers   []string
	cfg       config.Cluster
	reconcile ReconcileFunc
	log       *slog.Logger

	hbView     *heartbeatView
	hbProducer *producer.Producer
	elector    *elector
	placer     *placer

	mu    sync.Mutex
	base  []config.Pipeline // last config seen via ApplyConfig
	local map[string]int    // pipeline name -> workers assigned to this node; nil until the first placement record is observed
}

func New(brokers []string, cfg config.Cluster, reconcile ReconcileFunc, log *slog.Logger) *Node {
	id := cfg.NodeID
	if id == "" {
		id = defaultNodeID()
	}
	log = log.With("component", "cluster", "node_id", id)

	return &Node{
		id:        id,
		brokers:   brokers,
		cfg:       cfg,
		reconcile: reconcile,
		log:       log,
		hbView:    newHeartbeatView(),
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

// Start ensures the internal cluster topics exist and launches every
// background loop (heartbeat, election, placement). It returns once setup
// succeeds; the loops keep running until ctx is cancelled.
func (n *Node) Start(ctx context.Context) error {
	if err := kafkaadmin.EnsureCompactedTopic(ctx, n.brokers, HeartbeatTopic, 1); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", HeartbeatTopic, err)
	}
	if err := kafkaadmin.EnsureTopic(ctx, n.brokers, ElectionTopic, 1); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", ElectionTopic, err)
	}
	if err := kafkaadmin.EnsureCompactedTopic(ctx, n.brokers, PlacementTopic, 1); err != nil {
		return fmt.Errorf("ensuring %s exists: %w", PlacementTopic, err)
	}

	n.hbProducer = producer.New(n.brokers, HeartbeatTopic)
	go runHeartbeatProducer(ctx, n.hbProducer, n.id, time.Duration(n.cfg.HeartbeatIntervalSeconds)*time.Second, n.log)
	go n.hbView.run(ctx, n.brokers, n.log)

	n.placer = newPlacer(n.brokers, n.log)
	go n.placer.watch(ctx, n.onPlacementUpdate)

	el, err := newElector(n.brokers, time.Duration(n.cfg.NodeTimeoutSeconds)*time.Second, n.log)
	if err != nil {
		return fmt.Errorf("starting leader election: %w", err)
	}
	n.elector = el
	go el.run(ctx, n.runLeaderDuties)

	return nil
}

// runLeaderDuties is invoked once per election generation this node holds
// the election partition for. It recomputes and republishes placement on a
// fixed interval, until genCtx ends (leadership lost to a rebalance).
func (n *Node) runLeaderDuties(genCtx context.Context) {
	n.log.Info("became cluster leader")
	defer n.log.Info("lost cluster leadership")

	ticker := time.NewTicker(time.Duration(n.cfg.PlacementIntervalSeconds) * time.Second)
	defer ticker.Stop()

	publish := func() {
		n.mu.Lock()
		pipelines := n.base
		n.mu.Unlock()

		live := n.hbView.liveNodes(time.Duration(n.cfg.NodeTimeoutSeconds) * time.Second)
		if len(live) == 0 {
			live = []string{n.id} // a leader is by definition live; never publish an empty placement
		}
		if err := n.placer.publish(genCtx, pipelines, live); err != nil {
			n.log.Error("publishing placement failed", "error", err)
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
// (re)loads: once at startup, and again on every hot-reload. It stores the
// pipelines as the cluster-wide desired state and reconciles this node's
// local runners against the latest known placement.
func (n *Node) ApplyConfig(pipelines []config.Pipeline) []error {
	n.mu.Lock()
	n.base = pipelines
	n.mu.Unlock()
	return n.reconcileLocal(pipelines)
}

func (n *Node) onPlacementUpdate(assignments map[string]map[string]int) {
	local := make(map[string]int, len(assignments))
	for pipeline, byNode := range assignments {
		if w, ok := byNode[n.id]; ok {
			local[pipeline] = w
		}
	}

	n.mu.Lock()
	n.local = local
	pipelines := n.base
	n.mu.Unlock()

	n.reconcileLocal(pipelines)
}

// reconcileLocal narrows each pipeline's Workers to this node's share of the
// current placement decision, then reconciles.
//
// A pipeline the leader hasn't decided on yet (no placement observed at
// all, or this specific pipeline missing from a placement that has arrived)
// keeps its full configured Workers, the same as single-node mode:
// over-provisioning a few idle consumers before placement stabilizes is
// harmless, since Kafka's own group coordinator still only ever hands each
// partition to one consumer. Under-provisioning would instead leave a
// pipeline unprocessed, which is the failure mode worth avoiding.
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
				continue // the leader assigned this pipeline entirely to other nodes
			}
			p.Workers = w
		}
		adjusted = append(adjusted, p)
	}
	return n.reconcile(adjusted)
}

func (n *Node) StatusSnapshot() Status {
	live := n.hbView.liveNodes(time.Duration(n.cfg.NodeTimeoutSeconds) * time.Second)
	sort.Strings(live)
	leader := n.elector != nil && n.elector.IsLeader()
	return Status{NodeID: n.id, Leader: leader, LiveNodes: live}
}
