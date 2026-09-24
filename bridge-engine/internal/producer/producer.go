package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// batchTimeout replaces kafka-go's 1s default. Every Send is synchronous,
// and a partly-filled batch only flushes on this timeout, so the default
// added up to a second of latency to every single produce.
const batchTimeout = 5 * time.Millisecond

func New(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			BatchTimeout:           batchTimeout,
		},
	}
}

// SendMany writes all messages in one request, succeeding or failing
// together.
func (p *Producer) SendMany(ctx context.Context, msgs ...kafka.Message) error {
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("producing to %s: %w", p.writer.Topic, err)
	}
	return nil
}

func (p *Producer) Send(ctx context.Context, key, value []byte, headers map[string]string) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("producing to %s: %w", p.writer.Topic, err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
