package targetpool

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type endpoint struct {
	url       string
	healthURL string
	healthy   atomic.Bool
	inFlight  atomic.Int64
}

type Pool struct {
	endpoints []*endpoint
	strategy  config.TargetStrategy
	client    *callback.Client
	interval  time.Duration
	rrCounter atomic.Uint64
}

func New(target config.Target, client *callback.Client) *Pool {
	urls := target.URLs
	if target.Mode == config.TargetModeSingleURL {
		urls = []string{target.URL}
	}

	p := &Pool{
		strategy: target.Strategy,
		client:   client,
		interval: time.Duration(target.HealthCheckSecs) * time.Second,
	}
	for i, u := range urls {
		ep := &endpoint{url: u}
		if i < len(target.HealthCheckURLs) {
			ep.healthURL = target.HealthCheckURLs[i]
		}
		ep.healthy.Store(true)
		p.endpoints = append(p.endpoints, ep)
	}
	return p
}

// Run health-checks every endpoint that has a health_check_urls entry, on
// target.health_check_interval_seconds. Endpoints with no health check
// configured are always treated as healthy. Blocks until ctx is done; call
// it once per pool, in its own goroutine. A no-op if nothing needs probing.
func (p *Pool) Run(ctx context.Context) {
	if p.interval <= 0 {
		return
	}
	hasHealthCheck := false
	for _, ep := range p.endpoints {
		if ep.healthURL != "" {
			hasHealthCheck = true
			break
		}
	}
	if !hasHealthCheck {
		return
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ep := range p.endpoints {
				if ep.healthURL == "" {
					continue
				}
				go p.probe(ctx, ep)
			}
		}
	}
}

func (p *Pool) probe(ctx context.Context, ep *endpoint) {
	probeCtx, cancel := context.WithTimeout(ctx, p.interval)
	defer cancel()
	ep.healthy.Store(p.client.Probe(probeCtx, ep.healthURL) == nil)
}

// Pick selects an endpoint using the pool's strategy, skipping any endpoint
// its health probe has marked down. partitionKey drives sticky_partition.
// release must be called with the call's outcome once the request finishes,
// so least_inflight has an accurate in-flight count. ok is false only when
// every endpoint is unhealthy.
func (p *Pool) Pick(partitionKey int) (url string, release func(), ok bool) {
	n := len(p.endpoints)
	if n == 0 {
		return "", nil, false
	}

	var chosen *endpoint
	switch p.strategy {
	case config.StrategyLeastInFlight:
		for _, ep := range p.endpoints {
			if !ep.healthy.Load() {
				continue
			}
			if chosen == nil || ep.inFlight.Load() < chosen.inFlight.Load() {
				chosen = ep
			}
		}
	case config.StrategyStickyPartition:
		start := partitionKey % n
		if start < 0 {
			start += n
		}
		for i := 0; i < n; i++ {
			ep := p.endpoints[(start+i)%n]
			if ep.healthy.Load() {
				chosen = ep
				break
			}
		}
	default: // round_robin
		start := int(p.rrCounter.Add(1) - 1)
		for i := 0; i < n; i++ {
			ep := p.endpoints[(start+i)%n]
			if ep.healthy.Load() {
				chosen = ep
				break
			}
		}
	}

	if chosen == nil {
		return "", nil, false
	}

	chosen.inFlight.Add(1)
	return chosen.url, func() { chosen.inFlight.Add(-1) }, true
}
