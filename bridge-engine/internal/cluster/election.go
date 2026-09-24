package cluster

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// elector wraps a Kafka consumer group over ElectionTopic, a topic with
// exactly one partition. Kafka's own group coordinator only ever hands that
// one partition to a single group member at a time, so whichever member
// holds it is the cluster leader; if that member dies, the coordinator's
// normal rebalance hands the partition to a survivor. No separate election
// protocol needs to be written; this just observes the outcome of Kafka's.
type elector struct {
	cg       *kafka.ConsumerGroup
	isLeader atomic.Bool
	log      *slog.Logger
}

func newElector(brokers []string, sessionTimeout time.Duration, log *slog.Logger) (*elector, error) {
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:             ElectionTopic,
		Brokers:        brokers,
		Topics:         []string{ElectionTopic},
		SessionTimeout: sessionTimeout,
		StartOffset:    kafka.LastOffset,
	})
	if err != nil {
		return nil, err
	}
	return &elector{cg: cg, log: log}, nil
}

// run drives the election loop until ctx is done, calling onLeadership
// every time this node becomes leader. onLeadership receives a context
// bound to that specific election generation: it is cancelled the instant
// leadership is lost (a rebalance moves the partition elsewhere, or the
// group session times out), so callers must select on it and return
// promptly rather than keep acting as leader past that point.
func (e *elector) run(ctx context.Context, onLeadership func(genCtx context.Context)) {
	defer e.cg.Close()

	for {
		gen, err := e.cg.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.isLeader.Store(false)
			e.log.Error("leader election: joining the next generation failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		leader := len(gen.Assignments[ElectionTopic]) > 0
		e.isLeader.Store(leader)
		if leader {
			gen.Start(onLeadership)
		}
	}
}

func (e *elector) IsLeader() bool {
	return e.isLeader.Load()
}
