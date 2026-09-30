package sink

import (
	"context"

	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
	Env     map[string]any
}

type Sink interface {
	Write(ctx context.Context, m Message) error
	Close() error
}

// Kafka writes to a topic through a producer it doesn't own.
type Kafka struct{ P *producer.Producer }

func (k Kafka) Write(ctx context.Context, m Message) error {
	return k.P.Send(ctx, m.Key, m.Value, m.Headers)
}

// Close leaves the producer open; whoever made it closes it.
func (Kafka) Close() error { return nil }
