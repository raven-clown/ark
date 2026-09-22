package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type Runner struct {
	pipeline config.Pipeline
	reader   *kafka.Reader
	dest     *producer.Producer
	client   *callback.Client
	log      *slog.Logger
}

func New(brokers []string, p config.Pipeline, log *slog.Logger) *Runner {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          p.SourceTopic,
		GroupID:        p.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0,
	})

	return &Runner{
		pipeline: p,
		reader:   reader,
		dest:     producer.New(brokers, p.DestinationTopic),
		client:   callback.NewClient(30 * time.Second),
		log:      log.With("pipeline", p.Name),
	}
}

func (r *Runner) Close() error {
	if err := r.reader.Close(); err != nil {
		return err
	}
	return r.dest.Close()
}

func (r *Runner) Run(ctx context.Context) error {
	for {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}

		if err := r.process(ctx, msg); err != nil {
			r.log.Error("processing message failed", "error", err, "offset", msg.Offset, "partition", msg.Partition)
			continue
		}

		if err := r.reader.CommitMessages(ctx, msg); err != nil {
			r.log.Error("committing offset failed", "error", err, "offset", msg.Offset, "partition", msg.Partition)
		}
	}
}

func (r *Runner) process(ctx context.Context, msg kafka.Message) error {
	correlationID, err := callback.NewCorrelationID()
	if err != nil {
		return err
	}

	log := r.log.With("correlation_id", correlationID, "offset", msg.Offset, "partition", msg.Partition)

	var attempt int
	var resp *callback.Response
	for attempt = 1; attempt <= r.pipeline.Retry.MaxAttempts; attempt++ {
		resp, err = r.client.Post(ctx, r.pipeline.Target.URL, correlationID, msg.Value)
		if err == nil && resp.Success() {
			break
		}

		log.Warn("callback attempt failed", "attempt", attempt, "error", err)
		if attempt < r.pipeline.Retry.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond):
			}
		}
	}

	if err != nil || resp == nil || !resp.Success() {
		return fmt.Errorf("callback to %s exhausted %d attempts", r.pipeline.Target.URL, r.pipeline.Retry.MaxAttempts)
	}

	headers := map[string]string{callback.CorrelationIDHeader: correlationID}
	if err := r.dest.Send(ctx, msg.Key, resp.Body, headers); err != nil {
		return fmt.Errorf("producing result: %w", err)
	}

	log.Info("message processed", "attempts", attempt, "status_code", resp.StatusCode)
	return nil
}
