package targetpool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func newClient() *callback.Client {
	return callback.NewClient(time.Second)
}

func TestSingleURLModeAlwaysPicksTheOneURL(t *testing.T) {
	p := New(config.Target{Mode: config.TargetModeSingleURL, URL: "http://app:8080"}, newClient())

	url, release, ok := p.Pick(0)
	if !ok || url != "http://app:8080" {
		t.Fatalf("expected the single url to be picked, got url=%q ok=%v", url, ok)
	}
	release()
}

func TestRoundRobinCyclesThroughEndpoints(t *testing.T) {
	p := New(config.Target{
		Mode:     config.TargetModeMultiURL,
		URLs:     []string{"http://a", "http://b", "http://c"},
		Strategy: config.StrategyRoundRobin,
	}, newClient())

	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		url, release, ok := p.Pick(0)
		if !ok {
			t.Fatalf("expected Pick to succeed on round %d", i)
		}
		seen[url] = true
		release()
	}
	if len(seen) != 3 {
		t.Fatalf("expected round_robin to visit all 3 endpoints across 3 picks, saw %v", seen)
	}
}

func TestStickyPartitionIsStableWhenAllHealthy(t *testing.T) {
	p := New(config.Target{
		Mode:     config.TargetModeMultiURL,
		URLs:     []string{"http://a", "http://b", "http://c"},
		Strategy: config.StrategyStickyPartition,
	}, newClient())

	first, release, ok := p.Pick(7)
	if !ok {
		t.Fatal("expected Pick to succeed")
	}
	release()

	for i := 0; i < 5; i++ {
		url, release, ok := p.Pick(7)
		if !ok || url != first {
			t.Fatalf("expected partition 7 to keep sticking to %q, got %q", first, url)
		}
		release()
	}
}

func TestStickyPartitionFallsBackWhenPrimaryUnhealthy(t *testing.T) {
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer unhealthy.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()

	p := New(config.Target{
		Mode:            config.TargetModeMultiURL,
		URLs:            []string{"http://a", "http://b"},
		HealthCheckURLs: []string{unhealthy.URL, healthy.URL},
		HealthCheckSecs: 1,
		Strategy:        config.StrategyStickyPartition,
	}, newClient())

	p.endpoints[0].healthy.Store(false)

	url, release, ok := p.Pick(0)
	if !ok {
		t.Fatal("expected Pick to fall back to the healthy endpoint")
	}
	defer release()
	if url != "http://b" {
		t.Fatalf("expected fallback to http://b, got %q", url)
	}
}

func TestLeastInFlightPrefersTheEmptierEndpoint(t *testing.T) {
	p := New(config.Target{
		Mode:     config.TargetModeMultiURL,
		URLs:     []string{"http://a", "http://b"},
		Strategy: config.StrategyLeastInFlight,
	}, newClient())

	_, releaseA, ok := p.Pick(0)
	if !ok {
		t.Fatal("expected first pick to succeed")
	}
	// http://a now has 1 in flight; the next pick should prefer http://b.
	url, releaseB, ok := p.Pick(0)
	if !ok {
		t.Fatal("expected second pick to succeed")
	}
	if url != "http://b" {
		t.Fatalf("expected least_inflight to prefer the emptier endpoint http://b, got %q", url)
	}
	releaseA()
	releaseB()
}

func TestPickFailsWhenEveryEndpointIsUnhealthy(t *testing.T) {
	p := New(config.Target{
		Mode:     config.TargetModeMultiURL,
		URLs:     []string{"http://a", "http://b"},
		Strategy: config.StrategyRoundRobin,
	}, newClient())
	for _, ep := range p.endpoints {
		ep.healthy.Store(false)
	}

	if _, _, ok := p.Pick(0); ok {
		t.Fatal("expected Pick to fail when every endpoint is unhealthy")
	}
}

func TestRunProbesAndUpdatesHealth(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer down.Close()

	p := New(config.Target{
		Mode:            config.TargetModeMultiURL,
		URLs:            []string{"http://a"},
		HealthCheckURLs: []string{down.URL},
		HealthCheckSecs: 1,
		Strategy:        config.StrategyRoundRobin,
	}, newClient())

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	go p.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, ok := p.Pick(0); !ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected the endpoint to be marked unhealthy after Run probes it")
}

func TestRunIsNoOpWithoutHealthChecks(t *testing.T) {
	p := New(config.Target{Mode: config.TargetModeSingleURL, URL: "http://app"}, newClient())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected Run to return promptly when no endpoint has a health check configured")
	}
}
