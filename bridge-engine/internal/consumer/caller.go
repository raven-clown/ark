package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/breaker"
	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/metrics"
	"github.com/raven-clown/ark/bridge-engine/internal/tap"
	"github.com/raven-clown/ark/bridge-engine/internal/targetpool"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

// caller calls one HTTP target with retries, a circuit breaker and health
// checks. The fixed path has one for the pipeline's target; a flow has one
// per call step.
type caller struct {
	step    string
	target  config.Target
	retry   config.Retry
	client  *callback.Client
	breaker *breaker.Breaker
	pool    *targetpool.Pool
}

func newCaller(step string, t config.Target, r config.Retry, cb config.CircuitBreaker) *caller {
	client := callback.NewClient(time.Duration(t.TimeoutMs) * time.Millisecond)
	return &caller{
		step:    step,
		target:  t,
		retry:   r,
		client:  client,
		breaker: breaker.New(cb.FailureThreshold, time.Duration(cb.CooldownSeconds)*time.Second),
		pool:    targetpool.New(t, client),
	}
}

// callOutcome is how a call ended: answered (resp set, neither flag), a
// reject status, or every attempt used up.
type callOutcome struct {
	resp     *callback.Response
	attempts int
	rejected bool
	failed   bool
	reason   string
}

// call posts value until the target answers something final. It returns an
// error only when ctx ends.
func (r *Runner) call(ctx context.Context, c *caller, partition int, correlationID string, key, value []byte, log *slog.Logger) (callOutcome, error) {
	var out callOutcome
	var lastFailure string
	for out.attempts < c.retry.MaxAttempts {
		if !c.breaker.Allow() {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("callback blocked, circuit open, waiting for it to close before retrying this message", "step", c.step)
			if err := sleep(ctx, time.Second); err != nil {
				return out, err
			}
			continue
		}

		url, release, ok := c.pool.Pick(partition)
		if !ok {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("callback blocked, every target.urls endpoint is unhealthy, waiting before retrying this message", "step", c.step)
			if err := sleep(ctx, time.Second); err != nil {
				return out, err
			}
			continue
		}

		out.attempts++
		callStart := time.Now()
		resp, err := c.client.Post(ctx, url, correlationID, value)
		release()
		elapsed := time.Since(callStart)
		r.counters.callbackNanos.Add(elapsed.Nanoseconds())
		r.counters.callbackCount.Add(1)
		metrics.CallbackDuration.WithLabelValues(r.pipeline.Name, r.pipeline.Tenant).Observe(elapsed.Seconds())
		if r.watching() {
			rec := r.tapRecord(tap.StageCallback, correlationID, key, nil)
			rec.Target, rec.Attempt, rec.DurationMs, rec.Rule = url, out.attempts, float64(elapsed.Microseconds())/1000, c.step
			if resp != nil {
				rec.Status = resp.StatusCode
			}
			if err != nil {
				rec.Reason = err.Error()
			}
			tap.Default.Publish(rec)
		}

		if err == nil && c.target.IsReject(resp.StatusCode) {
			r.recordBreaker(c.breaker, true, "")
			out.resp, out.rejected = resp, true
			out.reason = fmt.Sprintf("target %s answered status %d, which is a reject status", url, resp.StatusCode)
			return out, nil
		}

		if err == nil && resp.RetryLater() {
			// An explicit "come back later" is not a failed attempt: it
			// neither spends a retry nor sends the message to the DLQ.
			out.attempts--
			wait := resp.RetryAfter
			if wait <= 0 {
				wait = time.Duration(c.retry.BackoffMs) * time.Millisecond
			}
			wait = min(wait, tuning.RetryAfterCap())
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("target asked to retry later", "status_code", resp.StatusCode, "retry_in", wait.String())
			events.Record(r.pipeline.Name, events.TargetRateLimited, fmt.Sprintf("target %s answered %d, waiting %s before trying again", url, resp.StatusCode, wait), map[string]string{"partition": strconv.Itoa(partition)})
			if err := sleep(ctx, wait); err != nil {
				return out, err
			}
			continue
		}

		if err == nil && resp.Success() {
			r.recordBreaker(c.breaker, true, "")
			out.resp = resp
			return out, nil
		}

		if err != nil {
			lastFailure = err.Error()
		} else {
			lastFailure = fmt.Sprintf("target %s answered status %d", url, resp.StatusCode)
		}
		if out.attempts > 1 {
			r.recordBreakerRepeat(c.breaker, lastFailure)
		} else {
			r.recordBreaker(c.breaker, false, lastFailure)
		}
		log.Warn("callback attempt failed", "attempt", out.attempts, "error", lastFailure, "step", c.step)
		if out.attempts < c.retry.MaxAttempts {
			if err := sleep(ctx, time.Duration(c.retry.BackoffMs)*time.Millisecond); err != nil {
				return out, err
			}
		}
	}
	out.failed = true
	out.reason = fmt.Sprintf("callback failed %d time(s), max_attempts reached; last failure: %s", out.attempts, lastFailure)
	return out, nil
}

// probeHealth closes c's breaker as soon as its health check answers again.
func (r *Runner) probeHealth(ctx context.Context, c *caller) {
	interval := time.Duration(c.target.HealthCheckSecs) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.breaker.State() != "open" {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, interval)
			err := c.client.Probe(probeCtx, c.target.HealthCheckURL)
			cancel()
			if err != nil {
				r.log.Debug("health check probe failed", "error", err)
				continue
			}
			r.log.Info("health check probe succeeded, closing circuit breaker", "step", c.step)
			r.recordBreaker(c.breaker, true, "health check "+c.target.HealthCheckURL+" answered")
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
