package cluster

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// elector wraps a Kafka consumer group over a topic with exactly one
// partition. Kafka's group coordinator only ever hands that partition to a
// single member at a time, so whichever member holds it is the leader; if
// it dies, the coordinator's normal rebalance hands the partition to a
// survivor.
//
// Holding the partition is not by itself a fence: a deposed leader can
// still produce for a moment after a rebalance. The generation ID passed to
// onLeadership is used as a placement epoch so readers can discard writes
// from an older leader.
type elector struct {
	cg       *kafka.ConsumerGroup
	topic    string
	isLeader atomic.Bool
	log      *slog.Logger
}

func newElector(brokers []string, topic string, sessionTimeout time.Duration, log *slog.Logger) (*elector, error) {
	cg, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:             topic,
		Brokers:        brokers,
		Topics:         []string{topic},
		SessionTimeout: sessionTimeout,
		StartOffset:    kafka.LastOffset,
	})
	if err != nil {
		return nil, err
	}
	return &elector{cg: cg, topic: topic, log: log}, nil
}

// run drives the election loop until ctx is done, calling onLeadership each
// time this node becomes leader. Its context ends the instant leadership is
// lost, so callers must select on it and return promptly.
func (e *elector) run(ctx context.Context, onLeadership func(genCtx context.Context, generation int32)) {
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

		leader := len(gen.Assignments[e.topic]) > 0
		e.isLeader.Store(leader)
		if leader {
			generation := gen.ID
			gen.Start(func(genCtx context.Context) {
				onLeadership(genCtx, generation)
				e.isLeader.Store(false)
			})
		}
	}
}

func (e *elector) IsLeader() bool {
	return e.isLeader.Load()
}
