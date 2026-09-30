package cluster

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

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
