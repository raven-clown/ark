package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/breaker"
	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type Runner struct {
	pipeline config.Pipeline
	reader   *kafka.Reader
	dest     *producer.Producer
	dlq      *producer.Producer
	reject   *producer.Producer
	client   *callback.Client
	breaker  *breaker.Breaker
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
		Logger:         kafka.LoggerFunc(func(f string, a ...interface{}) { log.Debug(fmt.Sprintf(f, a...)) }),
		ErrorLogger:    kafka.LoggerFunc(func(f string, a ...interface{}) { log.Error(fmt.Sprintf(f, a...)) }),
	})

	r := &Runner{
		pipeline: p,
		reader:   reader,
		dest:     producer.New(brokers, p.DestinationTopic),
		client:   callback.NewClient(30 * time.Second),
		breaker:  breaker.New(5, 30*time.Second),
		log:      log.With("pipeline", p.Name),
	}

	if p.DeadLetterTopic != "" {
		r.dlq = producer.New(brokers, p.DeadLetterTopic)
	}
	if p.RejectTopic != "" {
		r.reject = producer.New(brokers, p.RejectTopic)
	}

	return r
}

func (r *Runner) Close() error {
	err := r.reader.Close()
	if closeErr := r.dest.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if r.dlq != nil {
		if closeErr := r.dlq.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if r.reject != nil {
		if closeErr := r.reject.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

type job struct {
	msg  kafka.Message
	done chan error
}

func (r *Runner) Run(ctx context.Context) error {
	maxInFlight := r.pipeline.Concurrency.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 1
	}

	sem := make(chan struct{}, maxInFlight)
	commitQueue := make(chan *job, maxInFlight)
	commitErrCh := make(chan error, 1)

	go r.commitInOrder(ctx, commitQueue, commitErrCh)

	for {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			close(commitQueue)
			if ctx.Err() != nil {
				<-commitErrCh
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}

		j := &job{msg: msg, done: make(chan error, 1)}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			close(commitQueue)
			<-commitErrCh
			return nil
		}

		select {
		case commitQueue <- j:
		case <-ctx.Done():
			<-sem
			close(commitQueue)
			<-commitErrCh
			return nil
		}

		go func(j *job) {
			defer func() { <-sem }()
			j.done <- r.process(ctx, j.msg)
		}(j)
	}
}

func (r *Runner) commitInOrder(ctx context.Context, queue chan *job, done chan<- error) {
	for j := range queue {
		err := <-j.done
		if err != nil {
			r.log.Error("processing message failed", "error", err, "offset", j.msg.Offset, "partition", j.msg.Partition)
			continue
		}
		if err := r.reader.CommitMessages(ctx, j.msg); err != nil {
			r.log.Error("committing offset failed", "error", err, "offset", j.msg.Offset, "partition", j.msg.Partition)
		}
	}
	done <- nil
}

func (r *Runner) process(ctx context.Context, msg kafka.Message) error {
	correlationID, err := callback.NewCorrelationID()
	if err != nil {
		return err
	}

	log := r.log.With("correlation_id", correlationID, "offset", msg.Offset, "partition", msg.Partition)
	headers := map[string]string{callback.CorrelationIDHeader: correlationID}

	var attempt int
	var resp *callback.Response
	var lastErr error

	for attempt = 1; attempt <= r.pipeline.Retry.MaxAttempts; attempt++ {
		if !r.breaker.Allow() {
			lastErr = fmt.Errorf("circuit breaker open for %s", r.pipeline.Target.URL)
			log.Warn("callback skipped, circuit open", "attempt", attempt)
			break
		}

		resp, lastErr = r.client.Post(ctx, r.pipeline.Target.URL, correlationID, msg.Value)

		if lastErr == nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			r.breaker.RecordResult(true)
			return r.route(ctx, r.reject, msg.Key, msg.Value, headers, log, "rejected", resp.StatusCode)
		}

		success := lastErr == nil && resp.Success()
		r.breaker.RecordResult(success)
		if success {
			break
		}

		log.Warn("callback attempt failed", "attempt", attempt, "error", lastErr)
		if attempt < r.pipeline.Retry.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond):
			}
		}
	}

	if lastErr != nil || resp == nil || !resp.Success() {
		return r.route(ctx, r.dlq, msg.Key, msg.Value, headers, log, "dead_lettered", 0)
	}

	if err := r.dest.Send(ctx, msg.Key, resp.Body, headers); err != nil {
		return fmt.Errorf("producing result: %w", err)
	}

	log.Info("message processed", "attempts", attempt, "status_code", resp.StatusCode)
	return nil
}

func (r *Runner) route(ctx context.Context, target *producer.Producer, key, value []byte, headers map[string]string, log *slog.Logger, outcome string, statusCode int) error {
	if target == nil {
		return fmt.Errorf("%s but no topic configured for pipeline %s", outcome, r.pipeline.Name)
	}
	if err := target.Send(ctx, key, value, headers); err != nil {
		return fmt.Errorf("routing to %s: %w", outcome, err)
	}
	log.Info("message "+outcome, "status_code", statusCode)
	return nil
}
