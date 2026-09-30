package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
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

type caller struct {
	step    string
	headers map[string]string
	target  config.Target
	retry   config.Retry
	client  *callback.Client
	breaker *breaker.Breaker
	pool    *targetpool.Pool
	batch   *batcher
}

func newCaller(step string, t config.Target, r config.Retry, cb config.CircuitBreaker) *caller {
	client := callback.NewClient(time.Duration(t.TimeoutMs) * time.Millisecond)
	c := &caller{
		step:    step,
		target:  t,
		retry:   r,
		client:  client,
		breaker: breaker.New(cb.FailureThreshold, time.Duration(cb.CooldownSeconds)*time.Second),
		pool:    targetpool.New(t, client),
	}
	if t.Batched() {
		c.batch = newBatcher(c)
	}
	return c
}

type callOutcome struct {
	resp     *callback.Response
	attempts int
	rejected bool
	failed   bool
	reason   string
}

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

		resp, url, elapsed, err, ok := c.send(ctx, partition, correlationID, key, value)
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		if !ok {
			metrics.Backpressured.WithLabelValues(r.pipeline.Name, r.workerLabel, r.pipeline.Tenant).Inc()
			log.Warn("callback blocked, every target.urls endpoint is unhealthy, waiting before retrying this message", "step", c.step)
			if err := sleep(ctx, time.Second); err != nil {
				return out, err
			}
			continue
		}

		out.attempts++
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

func (c *caller) send(ctx context.Context, partition int, correlationID string, key, value []byte) (*callback.Response, string, time.Duration, error, bool) {
	if c.batch != nil {
		res, err := c.batch.submit(ctx, &batchItem{id: correlationID, key: key, value: value, partition: partition})
		if err != nil {
			return nil, "", 0, err, true
		}
		return res.resp, res.url, res.elapsed, res.err, !res.noURL
	}
	url, release, ok := c.pool.Pick(partition)
	if !ok {
		return nil, "", 0, nil, false
	}
	start := time.Now()
	resp, err := c.client.Send(ctx, http.MethodPost, url, correlationID, value, expandHeaders(c.headers))
	release()
	return resp, url, time.Since(start), err, true
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

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandHeaders fills ${NAME} in header values from the environment.
func expandHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = envRef.ReplaceAllStringFunc(v, func(ref string) string { return os.Getenv(ref[2 : len(ref)-1]) })
	}
	return out
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
