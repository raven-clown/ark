package cluster

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// The control topic carries operator intent that must hold on every node
// and survive restarts. Today that's pause/resume: pausing through any node
// pauses the pipeline everywhere, including on nodes that only start
// running it later.
const pauseKeyPrefix = "pause/"

var ErrUnknownPipeline = errors.New("pipeline is not configured in this cluster")

func (n *Node) watchControl(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     n.brokers,
		Topic:       n.topics.Control,
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
			n.log.Error("reading control topic failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		name, ok := strings.CutPrefix(string(msg.Key), pauseKeyPrefix)
		if !ok || n.onPause == nil {
			continue
		}
		n.onPause(name, string(msg.Value) == "1")
	}
}

// PublishPause records a cluster-wide pause or resume for pipeline name.
// Every node, this one included, applies it when it reads it back.
func (n *Node) PublishPause(ctx context.Context, name string, paused bool) error {
	if !n.configuredPipeline(name) {
		return ErrUnknownPipeline
	}
	val := "0"
	if paused {
		val = "1"
	}
	return n.control.Send(ctx, []byte(pauseKeyPrefix+name), []byte(val), nil)
}

func (n *Node) configuredPipeline(name string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.base {
		if p.Name == name {
			return true
		}
	}
	return false
}

// PipelineView is one pipeline's cluster-wide numbers, with the per-node
// breakdown they were summed from.
type PipelineView struct {
	Total PipelineStats            `json:"total"`
	Nodes map[string]PipelineStats `json:"nodes"`
}

// ClusterPipelines sums every live node's latest heartbeat stats.
func (n *Node) ClusterPipelines() map[string]PipelineView {
	out := make(map[string]PipelineView)
	for node, rec := range n.hbView.liveInfo(n.nodeTimeout()) {
		for name, st := range rec.Pipelines {
			v, ok := out[name]
			if !ok {
				v = PipelineView{Nodes: make(map[string]PipelineStats)}
			}
			v.Nodes[node] = st
			v.Total.Workers += st.Workers
			v.Total.Processed += st.Processed
			v.Total.Rejected += st.Rejected
			v.Total.DeadLettered += st.DeadLettered
			v.Total.Failed += st.Failed
			v.Total.Paused = v.Total.Paused || st.Paused
			out[name] = v
		}
	}
	return out
}
