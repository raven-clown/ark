package cluster

import (
	"context"
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/kafkatail"
)

const pauseKeyPrefix = "pause/"

var ErrUnknownPipeline = errors.New("pipeline is not configured in this cluster")

func (n *Node) watchControl(ctx context.Context) {
	kafkatail.Compacted(ctx, n.brokers, n.topics.Control, n.log, func(msg kafka.Message) {
		name, ok := strings.CutPrefix(string(msg.Key), pauseKeyPrefix)
		if !ok || n.onPause == nil {
			return
		}
		n.onPause(name, string(msg.Value) == "1")
	}, nil)
}

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
			if calls := v.Total.CallbackCalls + st.CallbackCalls; calls > 0 {
				v.Total.AvgCallbackMs = (v.Total.AvgCallbackMs*float64(v.Total.CallbackCalls) + st.AvgCallbackMs*float64(st.CallbackCalls)) / float64(calls)
			}
			v.Total.CallbackCalls += st.CallbackCalls
			v.Total.Running += st.Running
			v.Total.Lag += st.Lag
			v.Total.BreakerOpen = v.Total.BreakerOpen || st.BreakerOpen
			for le, n := range st.LatencyBuckets {
				if v.Total.LatencyBuckets == nil {
					v.Total.LatencyBuckets = make(map[string]uint64)
				}
				v.Total.LatencyBuckets[le] += n
			}
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
