// Package sink holds the places a message can be written to. Kafka is one;
// a database table is another. Flow steps write through Sink, so a new kind
// of destination is a new implementation rather than a new code path.
package sink

import (
	"context"

	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

// Message is what a sink writes: the message as it is at that step, and its
// parsed form for sinks that pick fields out of it.
type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
	// Env is what templates read: data, original, response, reason, key
	// and headers.
	Env map[string]any
}

// Sink writes one message. Write is safe to call from several goroutines
// and returns an error the caller may retry.
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
