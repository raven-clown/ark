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
	"github.com/raven-clown/ark/bridge-engine/internal/datarules"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/metrics"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
	"github.com/raven-clown/ark/bridge-engine/internal/rules"
	"github.com/raven-clown/ark/bridge-engine/internal/targetpool"
)

type counters struct {
	processed        atomic.Int64
	rejected         atomic.Int64
	deadLettered     atomic.Int64
	failed           atomic.Int64
	running          atomic.Bool
	lastActivityUnix atomic.Int64
	callbackNanos    atomic.Int64
	callbackCount    atomic.Int64
	lag              atomic.Int64
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
	// Lag is how many messages on this worker's partitions are waiting.
	Lag int64 `json:"lag"`
	// OldestUncommittedSeconds is how long the message holding back this
	// worker's commits has been in progress (0 when nothing is stuck).
	OldestUncommittedSeconds float64 `json:"oldest_uncommitted_seconds"`
	// AvgCallbackMs is the mean callback latency since the worker started.
	AvgCallbackMs float64 `json:"avg_callback_ms"`
	CallbackCalls int64   `json:"callback_calls"`
	ConsumerGroup string  `json:"consumer_group"`
}

type shared struct {
	brokers       []string
	source        *producer.Producer
	dest          *producer.Producer
	dlq           *producer.Producer
	reject        *producer.Producer
	client        *callback.Client
	breaker       *breaker.Breaker
	targetPool    *targetpool.Pool
	rules         *rules.Engine
	dataRules     *datarules.Checker
	paused        atomic.Bool
	overrideMu    sync.Mutex
	overrideDest  map[string]*producer.Producer
	dlqBrowser    *dlq.Browser
	rejectBrowser *dlq.Browser
}

const dlqBrowserMaxEntries = 200

// Deps are the process-wide dependencies every pipeline shares.
type Deps struct {
	Brokers           []string
	ReplicationFactor int
	DLQState          *dlq.StateStore
}

func newShared(ctx context.Context, deps Deps, p config.Pipeline, log *slog.Logger) (*shared, error) {
	brokers, replicationFactor := deps.Brokers, deps.ReplicationFactor
	engine, err := rules.Compile(p)
	if err != nil {
		return nil, err
	}
	checker, err := datarules.New(p.DataRules)
	if err != nil {
		return nil, fmt.Errorf("data_rules: %w", err)
	}

	if p.DeadLetterTopic != "" {
		if err := kafkaadmin.EnsureTopic(ctx, brokers, p.DeadLetterTopic, 1, replicationFactor); err != nil {
			return nil, fmt.Errorf("ensuring dead_letter_topic %s exists: %w", p.DeadLetterTopic, err)
		}
	}
	if p.RejectTopic != "" {
		if err := kafkaadmin.EnsureTopic(ctx, brokers, p.RejectTopic, 1, replicationFactor); err != nil {
			return nil, fmt.Errorf("ensuring reject_topic %s exists: %w", p.RejectTopic, err)
		}
	}

	client := callback.NewClient(time.Duration(p.Target.TimeoutMs) * time.Millisecond)
	s := &shared{
		brokers:      brokers,
		source:       producer.New(brokers, p.SourceTopic),
		dest:         producer.New(brokers, p.DestinationTopic),
		client:       client,
		breaker:      breaker.New(p.CircuitBreaker.FailureThreshold, time.Duration(p.CircuitBreaker.CooldownSeconds)*time.Second),
		targetPool:   targetpool.New(p.Target, client),
		rules:        engine,
		dataRules:    checker,
		overrideDest: make(map[string]*producer.Producer),
	}

	if p.DeadLetterTopic != "" {
		s.dlq = producer.New(brokers, p.DeadLetterTopic)
		s.dlqBrowser = dlq.NewBrowser(brokers, p.DeadLetterTopic, deps.DLQState, dlqBrowserMaxEntries, s.source, log)
	}
	if p.RejectTopic != "" {
		s.reject = producer.New(brokers, p.RejectTopic)
		s.rejectBrowser = dlq.NewBrowser(brokers, p.RejectTopic, deps.DLQState, dlqBrowserMaxEntries, s.source, log)
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
	if closeErr := s.source.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
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
	if s.dlqBrowser != nil {
		if closeErr := s.dlqBrowser.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if s.rejectBrowser != nil {
		if closeErr := s.rejectBrowser.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
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
	headSince   atomic.Int64
}

func NewPipeline(ctx context.Context, deps Deps, p config.Pipeline, log *slog.Logger) ([]*Runner, error) {
	brokers, replicationFactor := deps.Brokers, deps.ReplicationFactor
	log = log.With("pipeline", p.Name, "tenant", p.Tenant)

	workers := p.Workers
	if workers < 1 {
		workers = 1
	}

	if err := kafkaadmin.EnsureTopic(ctx, brokers, p.SourceTopic, workers, replicationFactor); err != nil {
		return nil, fmt.Errorf("pipeline %q: ensuring source_topic %s exists: %w", p.Name, p.SourceTopic, err)
	}

	sh, err := newShared(ctx, deps, p, log)
	if err != nil {
		return nil, fmt.Errorf("pipeline %q: %w", p.Name, err)
	}

	go sh.targetPool.Run(ctx)

	if sh.dlqBrowser != nil {
		go sh.dlqBrowser.Run(ctx)
	}
	if sh.rejectBrowser != nil {
		go sh.rejectBrowser.Run(ctx)
	}

	runners := make([]*Runner, 0, workers)
	for w := 0; w < workers; w++ {
		runners = append(runners, newRunner(sh, p, w, log))
	}

	return runners, nil
}

func newRunner(sh *shared, p config.Pipeline, workerID int, log *slog.Logger) *Runner {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:                sh.brokers,
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
	return &Runner{
		pipeline:    p,
		workerID:    workerID,
		workerLabel: strconv.Itoa(workerID),
		reader:      reader,
		shared:      sh,
		log:         log.With("worker", workerID),
	}
}

// AddRunner creates one more worker for the same pipeline as sibling,
// sharing its producers, target pool, breaker and pause state, so a
// pipeline can scale up without being restarted.
func AddRunner(sibling *Runner, workerID int, log *slog.Logger) *Runner {
	log = log.With("pipeline", sibling.pipeline.Name, "tenant", sibling.pipeline.Tenant)
	return newRunner(sibling.shared, sibling.pipeline, workerID, log)
}

func (r *Runner) WorkerID() int { return r.workerID }

func (r *Runner) Tenant() string {
	return r.pipeline.Tenant
}

func (r *Runner) Name() string {
	return r.pipeline.Name
}

func (r *Runner) Status() Status {
	s := Status{
		Pipeline:      r.pipeline.Name,
		Worker:        r.workerID,
		Tenant:        r.pipeline.Tenant,
		MCPAccess:     string(r.pipeline.MCPAccess),
		SourceTopic:   r.pipeline.SourceTopic,
		Destination:   r.pipeline.DestinationTopic,
		Enabled:       r.pipeline.IsEnabled(),
		Running:       r.counters.running.Load(),
		BreakerState:  r.shared.breaker.State(),
		Processed:     r.counters.processed.Load(),
		Rejected:      r.counters.rejected.Load(),
		DeadLettered:  r.counters.deadLettered.Load(),
		Failed:        r.counters.failed.Load(),
		Paused:        r.shared.paused.Load(),
		Lag:           r.counters.lag.Load(),
		CallbackCalls: r.counters.callbackCount.Load(),
		ConsumerGroup: r.pipeline.ConsumerGroup,
	}
	if n := s.CallbackCalls; n > 0 {
		s.AvgCallbackMs = float64(r.counters.callbackNanos.Load()) / float64(n) / 1e6
	}
	if since := r.headSince.Load(); since != 0 {
		s.OldestUncommittedSeconds = time.Since(time.Unix(0, since)).Seconds()
	}
	if unix := r.counters.lastActivityUnix.Load(); unix != 0 {
		formatted := time.Unix(unix, 0).UTC().Format(time.RFC3339)
		s.LastActivityAt = &formatted
	}
	return s
}

func (r *Runner) DLQBrowser() *dlq.Browser {
	return r.shared.dlqBrowser
}

func (r *Runner) RejectBrowser() *dlq.Browser {
	return r.shared.rejectBrowser
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
	msg       kafka.Message
	done      chan error
	fetchedAt time.Time
}

// finalCommitTimeout bounds the commits made while a worker drains after
// its context is cancelled; those commits can't use the cancelled context.
const finalCommitTimeout = 10 * time.Second

// maxRetryBackoff caps the in-place retry delay for a message that can't be
// completed, so a recovered dependency is picked up within this long.
const maxRetryBackoff = 30 * time.Second

func (r *Runner) Run(ctx context.Context) error {
	r.counters.running.Store(true)
	metrics.WorkerUp.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Set(1)
	defer func() {
		r.counters.running.Store(false)
		metrics.WorkerUp.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Set(0)
		metrics.OldestUncommittedAge.DeleteLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant)
		metrics.ConsumerLag.DeleteLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant)
	}()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	maxInFlight := r.pipeline.Concurrency.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 1
	}

	sem := make(chan struct{}, maxInFlight)
	commitQueue := make(chan *job, maxInFlight)
	commitErrCh := make(chan error, 1)
	lanes := newLanes()

	go r.commitInOrder(commitQueue, commitErrCh) // #nosec G118 -- final commits must outlive the cancelled worker ctx
	go r.reportLag(runCtx)
	if r.workerID == 0 && r.pipeline.Target.HealthCheckURL != "" {
		go r.probeHealth(runCtx)
	}

	drain := func() {
		close(commitQueue)
		<-commitErrCh
	}

	for {
		for r.shared.paused.Load() {
			select {
			case <-ctx.Done():
				drain()
				return nil
			case <-time.After(time.Second):
			}
		}

		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			// Stop in-flight jobs too, or drain would wait on messages that
			// retry in place forever and the worker would never restart.
			cancelRun()
			drain()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}

		j := &job{msg: msg, done: make(chan error, 1), fetchedAt: time.Now()}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			drain()
			return nil
		}

		select {
		case commitQueue <- j:
		case <-ctx.Done():
			<-sem
			drain()
			return nil
		}

		wait, release := lanes.acquire(r.laneKey(msg))
		go func(j *job) {
			defer func() { <-sem }()
			defer release()
			if wait != nil {
				select {
				case <-wait:
				case <-runCtx.Done():
					j.done <- runCtx.Err()
					return
				}
			}
			j.done <- r.processUntilDone(runCtx, j.msg)
		}(j)
	}
}

// laneKey groups messages that must be processed one at a time, in fetch
// order. An empty key means no ordering constraint for that message.
func (r *Runner) laneKey(msg kafka.Message) string {
	switch r.pipeline.Ordering {
	case config.OrderingNone:
		return ""
	case config.OrderingPerPartition:
		return "p" + strconv.Itoa(msg.Partition)
	default:
		if len(msg.Key) == 0 {
			return ""
		}
		return strconv.Itoa(msg.Partition) + "/" + string(msg.Key)
	}
}

// processUntilDone never gives up on a message while the worker is running:
// a message that can't be completed (no DLQ to route to, a webhook override
// that keeps failing) is retried in place with backoff instead of being
// skipped, because committing any later offset on its partition would
// permanently lose it. It only returns an error when ctx is cancelled, in
// which case the message is left uncommitted and redelivered later.
func (r *Runner) processUntilDone(ctx context.Context, msg kafka.Message) error {
	backoff := time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond
	for {
		err := r.process(ctx, msg)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		r.counters.failed.Add(1)
		metrics.Failed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
		r.log.Error("message could not be completed, retrying it in place instead of skipping it", "error", err, "offset", msg.Offset, "partition", msg.Partition, "retry_in", backoff.String())
		events.Record(r.pipeline.Name, events.MessageRetrying, "a message could not be completed and is being retried in place, holding back later messages on its partition: "+err.Error(), map[string]string{"partition": strconv.Itoa(msg.Partition), "offset": strconv.FormatInt(msg.Offset, 10), "retry_in": backoff.String()})

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxRetryBackoff)
	}
}

// commitInOrder commits finished jobs in fetch order. Once any job ends
// without completing (only possible when the worker is stopping), nothing
// after it is committed either: Kafka's committed offset is a single resume
// point per partition, so committing past an unfinished message would
// silently drop it.
func (r *Runner) commitInOrder(queue chan *job, done chan<- error) {
	halted := false
	for j := range queue {
		r.headSince.Store(j.fetchedAt.UnixNano())
		err := <-j.done
		r.headSince.Store(0)

		now := time.Now()
		r.counters.lastActivityUnix.Store(now.Unix())
		metrics.LastActivityTimestamp.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Set(float64(now.Unix()))

		if halted {
			continue
		}
		if err != nil {
			halted = true
			continue
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), finalCommitTimeout)
		if err := r.reader.CommitMessages(commitCtx, j.msg); err != nil {
			r.log.Error("committing offset failed", "error", err, "offset", j.msg.Offset, "partition", j.msg.Partition)
		}
		cancel()
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
			r.recordBreaker(true, "health check "+r.pipeline.Target.HealthCheckURL+" answered")
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
			r.counters.lag.Store(stats.Lag)
			metrics.ConsumerLag.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Set(float64(stats.Lag))
			age := 0.0
			if since := r.headSince.Load(); since != 0 {
				age = time.Since(time.Unix(0, since)).Seconds()
			}
			metrics.OldestUncommittedAge.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Set(age)
			state := 0.0
			if r.shared.breaker.State() == "open" {
				state = 1.0
			}
			metrics.CircuitBreakerOpen.WithLabelValues(r.pipeline.Name, r.pipeline.Tenant).Set(state)
			paused := 0.0
			if r.shared.paused.Load() {
				paused = 1.0
			}
			metrics.Paused.WithLabelValues(r.pipeline.Name, r.pipeline.Tenant).Set(paused)
		}
	}
}

// maxRetryAfter caps how long a target's Retry-After header can hold a
// message, so a misconfigured target can't stall a partition for hours.
const maxRetryAfter = 5 * time.Minute

func (r *Runner) process(ctx context.Context, msg kafka.Message) error {
	correlationID := callback.MessageCorrelationID(msg.Topic, msg.Partition, msg.Offset)

	log := r.log.With("correlation_id", correlationID, "offset", msg.Offset, "partition", msg.Partition)
	headers := map[string]string{callback.CorrelationIDHeader: correlationID}
	for _, h := range msg.Headers {
		if h.Key == dlq.RedriveCountHeader {
			headers[h.Key] = string(h.Value)
		}
	}

	if c := r.shared.dataRules; c != nil {
		original := make(map[string]string, len(msg.Headers))
		for _, h := range msg.Headers {
			original[h.Key] = string(h.Value)
		}
		if vs := c.Check(msg.Key, original, msg.Value); len(vs) > 0 {
			reason := datarules.Summary(vs)
			for _, v := range vs {
				metrics.DataRuleViolations.WithLabelValues(r.pipeline.Name, v.Rule, r.pipeline.Tenant).Inc()
			}
			events.Record(r.pipeline.Name, events.DataRuleViolation, reason, map[string]string{
				"partition": strconv.Itoa(msg.Partition), "offset": strconv.FormatInt(msg.Offset, 10),
				"correlation_id": correlationID, "on_violation": c.OnViolation(),
			})
			switch c.OnViolation() {
			case "tag":
				headers[ViolationsHeader] = reason
			case "dead_letter":
				return r.route(ctx, r.shared.dlq, msg.Key, msg.Value, headers, log, "dead_lettered", 0, reason)
			default:
				return r.route(ctx, r.shared.reject, msg.Key, msg.Value, headers, log, "rejected", 0, reason)
			}
		}
	}

	if r.shared.rules.HasFastPath() {
		rule, err := r.shared.rules.EvaluateFastPath(msg.Value)
		if err != nil {
			log.Error("fast_path_rules evaluation failed", "error", err)
		} else if rule != nil {
			metrics.FastPathMatches.WithLabelValues(r.pipeline.Name, rule.Name, string(rule.Action), r.pipeline.Tenant).Inc()
			log.Info("fast_path_rule matched", "rule", rule.Name, "action", rule.Action)
			return r.applyRuleAction(ctx, rule, msg.Key, msg.Value, headers, log)
		}
	}

	var resp *callback.Response
	var lastErr error
	var lastFailure string
	realAttempts := 0

	for realAttempts < r.pipeline.Retry.MaxAttempts {
		if !r.shared.breaker.Allow() {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("callback blocked, circuit open, waiting for it to close before retrying this message")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		url, release, ok := r.shared.targetPool.Pick(msg.Partition)
		if !ok {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("callback blocked, every target.urls endpoint is unhealthy, waiting before retrying this message")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		realAttempts++
		callStart := time.Now()
		resp, lastErr = r.shared.client.Post(ctx, url, correlationID, msg.Value)
		release()
		elapsed := time.Since(callStart)
		r.counters.callbackNanos.Add(elapsed.Nanoseconds())
		r.counters.callbackCount.Add(1)
		metrics.CallbackDuration.WithLabelValues(r.pipeline.Name, r.pipeline.Tenant).Observe(elapsed.Seconds())

		if lastErr == nil && r.pipeline.Target.IsReject(resp.StatusCode) {
			r.recordBreaker(true, "")
			reason := fmt.Sprintf("target %s answered status %d, which is a reject status", url, resp.StatusCode)
			return r.route(ctx, r.shared.reject, msg.Key, msg.Value, headers, log, "rejected", resp.StatusCode, reason)
		}

		if lastErr == nil && resp.RetryLater() {
			// An explicit "come back later" is not a failed attempt: it
			// neither spends a retry nor sends the message to the DLQ.
			realAttempts--
			wait := resp.RetryAfter
			if wait <= 0 {
				wait = time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond
			}
			wait = min(wait, maxRetryAfter)
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("target asked to retry later", "status_code", resp.StatusCode, "retry_in", wait.String())
			events.Record(r.pipeline.Name, events.TargetRateLimited, fmt.Sprintf("target %s answered %d, waiting %s before trying again", url, resp.StatusCode, wait), map[string]string{"partition": strconv.Itoa(msg.Partition), "offset": strconv.FormatInt(msg.Offset, 10)})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}

		success := lastErr == nil && resp.Success()
		failure := ""
		if !success {
			if lastErr != nil {
				failure = lastErr.Error()
			} else {
				failure = fmt.Sprintf("target %s answered status %d", url, resp.StatusCode)
			}
			lastFailure = failure
		}
		r.recordBreaker(success, failure)
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
		reason := fmt.Sprintf("callback failed %d time(s), max_attempts reached; last failure: %s", realAttempts, lastFailure)
		return r.route(ctx, r.shared.dlq, msg.Key, msg.Value, headers, log, "dead_lettered", 0, reason)
	}

	if r.shared.rules.HasPostCallback() {
		rule, err := r.shared.rules.EvaluatePostCallback(msg.Value, resp.StatusCode, resp.Body)
		if err != nil {
			log.Error("post_callback_rules evaluation failed", "error", err)
		} else if rule != nil {
			metrics.PostCallbackMatches.WithLabelValues(r.pipeline.Name, rule.Name, string(rule.Action), r.pipeline.Tenant).Inc()
			log.Info("post_callback_rule matched", "rule", rule.Name, "action", rule.Action)
			return r.applyRuleAction(ctx, rule, msg.Key, resp.Body, headers, log)
		}
	}

	if err := r.sendWithRetry(ctx, r.shared.dest, msg.Key, resp.Body, headers, log); err != nil {
		return fmt.Errorf("producing result: %w", err)
	}

	r.counters.processed.Add(1)
	metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
	log.Info("message processed", "attempts", realAttempts, "status_code", resp.StatusCode)
	return nil
}

// Headers ARK adds to every message it sends to a reject or dead-letter
// topic, so whoever looks at it later can tell where it came from and why.
const (
	ReasonHeader   = "X-Ark-Reason"
	PipelineHeader = "X-Ark-Pipeline"
	FailedAtHeader = "X-Ark-Failed-At"
	// ViolationsHeader lists broken data rules on a message let through
	// with data_rules.on_violation: tag.
	ViolationsHeader = "X-Ark-Violations"
)

func (r *Runner) route(ctx context.Context, target *producer.Producer, key, value []byte, headers map[string]string, log *slog.Logger, outcome string, statusCode int, reason string) error {
	if target == nil && outcome == "rejected" {
		// No reject_topic: a rejected message still has to go somewhere
		// other than being skipped, and the DLQ is where it can be seen.
		target, outcome = r.shared.dlq, "dead_lettered"
		reason += " (no reject_topic, sent to the dead-letter topic instead)"
	}
	routed := make(map[string]string, len(headers)+3)
	for k, v := range headers {
		routed[k] = v
	}
	routed[ReasonHeader] = reason
	routed[PipelineHeader] = r.pipeline.Name
	routed[FailedAtHeader] = time.Now().UTC().Format(time.RFC3339)
	headers = routed
	if target == nil {
		// Only reachable with on_exhausted: block, which the operator chose
		// knowing the message is retried in place until the target accepts.
		return fmt.Errorf("%s but no topic configured for pipeline %s (on_exhausted: block keeps retrying it)", outcome, r.pipeline.Name)
	}
	if err := r.sendWithRetry(ctx, target, key, value, headers, log); err != nil {
		return fmt.Errorf("routing to %s: %w", outcome, err)
	}
	switch outcome {
	case "rejected":
		r.counters.rejected.Add(1)
		metrics.Rejected.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
		events.Record(r.pipeline.Name, events.MessageRejected, reason, map[string]string{"key": string(key), "correlation_id": headers[callback.CorrelationIDHeader]})
	case "dead_lettered":
		r.counters.deadLettered.Add(1)
		metrics.DeadLettered.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
		events.Record(r.pipeline.Name, events.MessageDeadLettered, reason, map[string]string{"key": string(key), "correlation_id": headers[callback.CorrelationIDHeader]})
	}
	log.Info("message "+outcome, "status_code", statusCode, "reason", reason)
	return nil
}

// recordBreaker feeds a callback result to the circuit breaker and records
// an event whenever that flips the breaker, with what caused it.
func (r *Runner) recordBreaker(success bool, cause string) {
	before := r.shared.breaker.State()
	r.shared.breaker.RecordResult(success)
	after := r.shared.breaker.State()
	if before == after {
		return
	}
	switch after {
	case "open":
		events.Record(r.pipeline.Name, events.BreakerOpened, "circuit breaker opened after repeated callback failures; messages now wait in place until the target recovers", map[string]string{"last_failure": cause})
	case "closed":
		msg := "circuit breaker closed, callbacks flowing again"
		if cause != "" {
			msg += " (" + cause + ")"
		}
		events.Record(r.pipeline.Name, events.BreakerClosed, msg, nil)
	}
}

func (r *Runner) applyRuleAction(ctx context.Context, rule *config.FastPathRule, key, value []byte, headers map[string]string, log *slog.Logger) error {
	switch rule.Action {
	case config.ActionPassThrough:
		if err := r.sendWithRetry(ctx, r.shared.dest, key, value, headers, log); err != nil {
			return fmt.Errorf("pass_through producing result: %w", err)
		}
		r.counters.processed.Add(1)
		metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
		return nil

	case config.ActionReject:
		return r.route(ctx, r.shared.reject, key, value, headers, log, "rejected", 0, fmt.Sprintf("rule %q matched with action reject", rule.Name))

	case config.ActionDrop:
		log.Info("message dropped", "rule", rule.Name)
		return nil

	case config.ActionDeadLetter:
		return r.route(ctx, r.shared.dlq, key, value, headers, log, "dead_lettered", 0, fmt.Sprintf("rule %q matched with action dead_letter", rule.Name))

	case config.ActionTransformRoute:
		if rule.DestinationOverride != "" {
			target := r.shared.overrideProducer(rule.DestinationOverride)
			if err := r.sendWithRetry(ctx, target, key, value, headers, log); err != nil {
				return fmt.Errorf("routing to %s: %w", rule.DestinationOverride, err)
			}
			r.counters.processed.Add(1)
			metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Info("message routed", "rule", rule.Name, "destination", rule.DestinationOverride)
			return nil
		}
		if err := r.webhookWithRetry(ctx, rule.WebhookOverride, headers[callback.CorrelationIDHeader], value, log); err != nil {
			return fmt.Errorf("routing to webhook %s: %w", rule.WebhookOverride, err)
		}
		r.counters.processed.Add(1)
		metrics.Processed.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
		log.Info("message routed", "rule", rule.Name, "webhook", rule.WebhookOverride)
		return nil

	default:
		return fmt.Errorf("rule %q: unhandled action %q", rule.Name, rule.Action)
	}
}

func (r *Runner) webhookWithRetry(ctx context.Context, url, correlationID string, value []byte, log *slog.Logger) error {
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

// sendWithRetry retries a produce until it succeeds or ctx ends. A produce
// failure means Kafka itself is unavailable, not that the message is bad,
// so giving up would only force the callback to be repeated later.
func (r *Runner) sendWithRetry(ctx context.Context, target *producer.Producer, key, value []byte, headers map[string]string, log *slog.Logger) error {
	backoff := time.Duration(r.pipeline.Retry.BackoffMs) * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := target.Send(ctx, key, value, headers)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Warn("produce attempt failed, retrying until Kafka accepts it", "attempt", attempt, "error", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxRetryBackoff)
	}
}
