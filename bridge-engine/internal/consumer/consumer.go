package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/breaker"
	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/metrics"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
	"github.com/raven-clown/ark/bridge-engine/internal/rules"
)

type counters struct {
	processed        atomic.Int64
	rejected         atomic.Int64
	deadLettered     atomic.Int64
	failed           atomic.Int64
	running          atomic.Bool
	lastActivityUnix atomic.Int64
}

type Status struct {
	Pipeline       string  `json:"pipeline"`
	Worker         int     `json:"worker"`
	Tenant         string  `json:"tenant"`
	MCPAccess      string  `json:"mcp_access"`
	SourceTopic    string  `json:"source_topic"`
	Destination    string  `json:"destination_topic,omitempty"`
	Enabled        bool    `json:"enabled"`
	Running        bool    `json:"running"`
	BreakerState   string  `json:"breaker_state"`
	Processed      int64   `json:"processed"`
	Rejected       int64   `json:"rejected"`
	DeadLettered   int64   `json:"dead_lettered"`
	Failed         int64   `json:"failed"`
	Paused         bool    `json:"paused"`
	LastActivityAt *string `json:"last_activity_at,omitempty"`
}

type shared struct {
	brokers      []string
	dest         *producer.Producer
	dlq          *producer.Producer
	reject       *producer.Producer
	client       *callback.Client
	breaker      *breaker.Breaker
	rules        *rules.Engine
	paused       atomic.Bool
	overrideMu   sync.Mutex
	overrideDest map[string]*producer.Producer
}

func newShared(brokers []string, p config.Pipeline) (*shared, error) {
	engine, err := rules.Compile(p)
	if err != nil {
		return nil, err
	}

	s := &shared{
		brokers:      brokers,
		dest:         producer.New(brokers, p.DestinationTopic),
		client:       callback.NewClient(30 * time.Second),
		breaker:      breaker.New(5, 30*time.Second),
		rules:        engine,
		overrideDest: make(map[string]*producer.Producer),
	}
	if p.DeadLetterTopic != "" {
		s.dlq = producer.New(brokers, p.DeadLetterTopic)
	}
	if p.RejectTopic != "" {
		s.reject = producer.New(brokers, p.RejectTopic)
	}
	return s, nil
}

func (s *shared) overrideProducer(topic string) *producer.Producer {
	s.overrideMu.Lock()
	defer s.overrideMu.Unlock()
	if p, ok := s.overrideDest[topic]; ok {
		return p
	}
	p := producer.New(s.brokers, topic)
	s.overrideDest[topic] = p
	return p
}

func (s *shared) Close() error {
	err := s.dest.Close()
	if s.dlq != nil {
		if closeErr := s.dlq.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.reject != nil {
		if closeErr := s.reject.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	s.overrideMu.Lock()
	for _, p := range s.overrideDest {
		if closeErr := p.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	s.overrideMu.Unlock()
	return err
}

type Runner struct {
	pipeline    config.Pipeline
	workerID    int
	workerLabel string
	reader      *kafka.Reader
	shared      *shared
	log         *slog.Logger
	counters    counters
}

func NewPipeline(ctx context.Context, brokers []string, p config.Pipeline, log *slog.Logger) ([]*Runner, error) {
	sh, err := newShared(brokers, p)
	if err != nil {
		return nil, fmt.Errorf("pipeline %q: %w", p.Name, err)
	}

	workers := p.Workers
	if workers < 1 {
		workers = 1
	}

	if err := kafkaadmin.EnsureTopic(ctx, brokers, p.SourceTopic, workers); err != nil {
		return nil, fmt.Errorf("pipeline %q: ensuring source_topic %s exists: %w", p.Name, p.SourceTopic, err)
	}

	runners := make([]*Runner, 0, workers)
	for w := 0; w < workers; w++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:                brokers,
			Topic:                  p.SourceTopic,
			GroupID:                p.ConsumerGroup,
			MinBytes:               1,
			MaxBytes:               10e6,
			MaxWait:                time.Second,
			CommitInterval:         0,
			WatchPartitionChanges:  true,
			PartitionWatchInterval: 5 * time.Second,
			Logger:                 kafka.LoggerFunc(func(f string, a ...interface{}) { log.Debug(fmt.Sprintf(f, a...)) }),
			ErrorLogger:            kafka.LoggerFunc(func(f string, a ...interface{}) { log.Error(fmt.Sprintf(f, a...)) }),
		})

		runners = append(runners, &Runner{
			pipeline:    p,
			workerID:    w,
			workerLabel: strconv.Itoa(w),
			reader:      reader,
			shared:      sh,
			log:         log.With("pipeline", p.Name, "worker", w),
		})
	}

	return runners, nil
}

func (r *Runner) Name() string {
	return r.pipeline.Name
}

func (r *Runner) Status() Status {
	s := Status{
		Pipeline:     r.pipeline.Name,
		Worker:       r.workerID,
		Tenant:       r.pipeline.Tenant,
		MCPAccess:    string(r.pipeline.MCPAccess),
		SourceTopic:  r.pipeline.SourceTopic,
		Destination:  r.pipeline.DestinationTopic,
		Enabled:      r.pipeline.IsEnabled(),
		Running:      r.counters.running.Load(),
		BreakerState: r.shared.breaker.State(),
		Processed:    r.counters.processed.Load(),
		Rejected:     r.counters.rejected.Load(),
		DeadLettered: r.counters.deadLettered.Load(),
		Failed:       r.counters.failed.Load(),
		Paused:       r.shared.paused.Load(),
	}
	if unix := r.counters.lastActivityUnix.Load(); unix != 0 {
		formatted := time.Unix(unix, 0).UTC().Format(time.RFC3339)
		s.LastActivityAt = &formatted
	}
	return s
}

func (r *Runner) Close() error {
	return r.reader.Close()
}

func CloseShared(runners []*Runner) error {
	if len(runners) == 0 {
		return nil
	}
	return runners[0].shared.Close()
}

func Pause(runners []*Runner) {
	if len(runners) == 0 {
		return
	}
	runners[0].shared.paused.Store(true)
}

func Resume(runners []*Runner) {
	if len(runners) == 0 {
		return
	}
	runners[0].shared.paused.Store(false)
}

type job struct {
	msg  kafka.Message
	done chan error
}

func (r *Runner) Run(ctx context.Context) error {
	r.counters.running.Store(true)
	metrics.WorkerUp.WithLabelValues(r.pipeline.Name, r.workerLabel).Set(1)
	defer func() {
		r.counters.running.Store(false)
		metrics.WorkerUp.WithLabelValues(r.pipeline.Name, r.workerLabel).Set(0)
	}()

	maxInFlight := r.pipeline.Concurrency.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 1
	}

	sem := make(chan struct{}, maxInFlight)
	commitQueue := make(chan *job, maxInFlight)
	commitErrCh := make(chan error, 1)

	go r.commitInOrder(ctx, commitQueue, commitErrCh)
	go r.reportLag(ctx)
	if r.workerID == 0 && r.pipeline.Target.HealthCheckURL != "" {
		go r.probeHealth(ctx)
	}

	for {
		for r.shared.paused.Load() {
			select {
			case <-ctx.Done():
				close(commitQueue)
				<-commitErrCh
				return nil
			case <-time.After(time.Second):
			}
		}

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

		now := time.Now()
		r.counters.lastActivityUnix.Store(now.Unix())
		metrics.LastActivityTimestamp.WithLabelValues(r.pipeline.Name, r.workerLabel).Set(float64(now.Unix()))
		if err != nil {
			r.counters.failed.Add(1)
			metrics.Failed.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
			r.log.Error("processing message failed", "error", err, "offset", j.msg.Offset, "partition", j.msg.Partition)
			continue
		}
		if err := r.reader.CommitMessages(ctx, j.msg); err != nil {
			r.log.Error("committing offset failed", "error", err, "offset", j.msg.Offset, "partition", j.msg.Partition)
		}
	}
	done <- nil
}

func (r *Runner) probeHealth(ctx context.Context) {
	interval := time.Duration(r.pipeline.Target.HealthCheckSecs) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.shared.breaker.State() != "open" {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, interval)
			err := r.shared.client.Probe(probeCtx, r.pipeline.Target.HealthCheckURL)
			cancel()
			if err != nil {
				r.log.Debug("health check probe failed", "error", err)
				continue
			}
			r.log.Info("health check probe succeeded, closing circuit breaker")
			r.shared.breaker.RecordResult(true)
		}
	}
}

func (r *Runner) reportLag(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := r.reader.Stats()
			metrics.ConsumerLag.WithLabelValues(r.pipeline.Name, r.workerLabel).Set(float64(stats.Lag))
			state := 0.0
			if r.shared.breaker.State() == "open" {
				state = 1.0
			}
			metrics.CircuitBreakerOpen.WithLabelValues(r.pipeline.Name).Set(state)
			paused := 0.0
			if r.shared.paused.Load() {
				paused = 1.0
			}
			metrics.Paused.WithLabelValues(r.pipeline.Name).Set(paused)
		}
	}
}

func (r *Runner) process(ctx context.Context, msg kafka.Message) error {
	correlationID, err := callback.NewCorrelationID()
	if err != nil {
		return err
	}

	log := r.log.With("correlation_id", correlationID, "offset", msg.Offset, "partition", msg.Partition)
	headers := map[string]string{callback.CorrelationIDHeader: correlationID}

	if r.shared.rules.HasFastPath() {
		rule, err := r.shared.rules.EvaluateFastPath(msg.Value)
		if err != nil {
			log.Error("fast_path_rules evaluation failed", "error", err)
		} else if rule != nil {
			metrics.FastPathMatches.WithLabelValues(r.pipeline.Name, rule.Name, string(rule.Action)).Inc()
			log.Info("fast_path_rule matched", "rule", rule.Name, "action", rule.Action)
			return r.applyRuleAction(ctx, rule, msg.Key, msg.Value, headers, log)
		}
	}

	var resp *callback.Response
	var lastErr error
	realAttempts := 0

	for realAttempts < r.pipeline.Retry.MaxAttempts {
		if !r.shared.breaker.Allow() {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
			log.Warn("callback blocked, circuit open, waiting for it to close before retrying this message")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		realAttempts++
		callStart := time.Now()
		resp, lastErr = r.shared.client.Post(ctx, r.pipeline.Target.URL, correlationID, msg.Value)
		metrics.CallbackDuration.WithLabelValues(r.pipeline.Name).Observe(time.Since(callStart).Seconds())

		if lastErr == nil && resp.StatusCode >= 400 && resp.StatusCode < 500 {
			r.shared.breaker.RecordResult(true)
			return r.route(ctx, r.shared.reject, msg.Key, msg.Value, headers, log, "rejected", resp.StatusCode)
		}

		success := lastErr == nil && resp.Success()
		r.shared.breaker.RecordResult(success)
		if success {
			break
		}

		log.Warn("callback attempt failed", "attempt", realAttempts, "error", lastErr)
		if realAttempts < r.pipeline.Retry.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond):
			}
		}
	}

	if lastErr != nil || resp == nil || !resp.Success() {
		return r.route(ctx, r.shared.dlq, msg.Key, msg.Value, headers, log, "dead_lettered", 0)
	}

	if r.shared.rules.HasPostCallback() {
		rule, err := r.shared.rules.EvaluatePostCallback(msg.Value, resp.StatusCode, resp.Body)
		if err != nil {
			log.Error("post_callback_rules evaluation failed", "error", err)
		} else if rule != nil {
			metrics.PostCallbackMatches.WithLabelValues(r.pipeline.Name, rule.Name, string(rule.Action)).Inc()
			log.Info("post_callback_rule matched", "rule", rule.Name, "action", rule.Action)
			return r.applyRuleAction(ctx, rule, msg.Key, resp.Body, headers, log)
		}
	}

	if err := r.sendWithRetry(ctx, r.shared.dest, msg.Key, resp.Body, headers, log); err != nil {
		return fmt.Errorf("producing result: %w", err)
	}

	r.counters.processed.Add(1)
	metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
	log.Info("message processed", "attempts", realAttempts, "status_code", resp.StatusCode)
	return nil
}

func (r *Runner) route(ctx context.Context, target *producer.Producer, key, value []byte, headers map[string]string, log *slog.Logger, outcome string, statusCode int) error {
	if target == nil {
		return fmt.Errorf("%s but no topic configured for pipeline %s", outcome, r.pipeline.Name)
	}
	if err := r.sendWithRetry(ctx, target, key, value, headers, log); err != nil {
		return fmt.Errorf("routing to %s: %w", outcome, err)
	}
	switch outcome {
	case "rejected":
		r.counters.rejected.Add(1)
		metrics.Rejected.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
	case "dead_lettered":
		r.counters.deadLettered.Add(1)
		metrics.DeadLettered.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
	}
	log.Info("message "+outcome, "status_code", statusCode)
	return nil
}

func (r *Runner) applyRuleAction(ctx context.Context, rule *config.FastPathRule, key, value []byte, headers map[string]string, log *slog.Logger) error {
	switch rule.Action {
	case config.ActionPassThrough:
		if err := r.sendWithRetry(ctx, r.shared.dest, key, value, headers, log); err != nil {
			return fmt.Errorf("pass_through producing result: %w", err)
		}
		r.counters.processed.Add(1)
		metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
		return nil

	case config.ActionReject:
		return r.route(ctx, r.shared.reject, key, value, headers, log, "rejected", 0)

	case config.ActionDrop:
		log.Info("message dropped", "rule", rule.Name)
		return nil

	case config.ActionDeadLetter:
		return r.route(ctx, r.shared.dlq, key, value, headers, log, "dead_lettered", 0)

	case config.ActionTransformRoute:
		if rule.DestinationOverride != "" {
			target := r.shared.overrideProducer(rule.DestinationOverride)
			if err := r.sendWithRetry(ctx, target, key, value, headers, log); err != nil {
				return fmt.Errorf("routing to %s: %w", rule.DestinationOverride, err)
			}
			r.counters.processed.Add(1)
			metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
			log.Info("message routed", "rule", rule.Name, "destination", rule.DestinationOverride)
			return nil
		}
		if err := r.webhookWithRetry(ctx, rule.WebhookOverride, value, log); err != nil {
			return fmt.Errorf("routing to webhook %s: %w", rule.WebhookOverride, err)
		}
		r.counters.processed.Add(1)
		metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel).Inc()
		log.Info("message routed", "rule", rule.Name, "webhook", rule.WebhookOverride)
		return nil

	default:
		return fmt.Errorf("rule %q: unhandled action %q", rule.Name, rule.Action)
	}
}

func (r *Runner) webhookWithRetry(ctx context.Context, url string, value []byte, log *slog.Logger) error {
	correlationID, err := callback.NewCorrelationID()
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= r.pipeline.Retry.MaxAttempts; attempt++ {
		var resp *callback.Response
		resp, lastErr = r.shared.client.Post(ctx, url, correlationID, value)
		if lastErr == nil && resp.Success() {
			return nil
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("webhook %s returned status %d", url, resp.StatusCode)
		}

		log.Warn("webhook attempt failed", "attempt", attempt, "error", lastErr)
		if attempt < r.pipeline.Retry.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond):
			}
		}
	}
	return lastErr
}

func (r *Runner) sendWithRetry(ctx context.Context, target *producer.Producer, key, value []byte, headers map[string]string, log *slog.Logger) error {
	var lastErr error
	for attempt := 1; attempt <= r.pipeline.Retry.MaxAttempts; attempt++ {
		lastErr = target.Send(ctx, key, value, headers)
		if lastErr == nil {
			return nil
		}

		log.Warn("produce attempt failed", "attempt", attempt, "error", lastErr)
		if attempt < r.pipeline.Retry.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond):
			}
		}
	}
	return lastErr
}
