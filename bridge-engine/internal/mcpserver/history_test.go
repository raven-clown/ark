package mcpserver

import (
	"math"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

func TestHistoryKeepsAnHourAndForgetsDeletedPipelines(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	now := time.Now()
	keep := int(tuning.HistoryKeep() / tuning.HistorySample())
	c.hist.sample(c.d, now)
	if n := len(c.hist.since("orders", time.Time{})); n != 0 {
		t.Fatalf("the first sample is only a baseline, got %d points", n)
	}
	for i := 1; i <= keep+10; i++ {
		c.hist.sample(c.d, now.Add(time.Duration(i)*tuning.HistorySample()))
	}
	if n := len(c.hist.pipes.data["orders"]); n != keep {
		t.Fatalf("kept %d points, want %d", n, keep)
	}
	src.p = nil
	c.hist.sample(c.d, now.Add(time.Hour*2))
	if n := len(c.hist.since("orders", time.Time{})); n != 0 {
		t.Fatalf("deleted pipeline still has %d points", n)
	}
	if len(c.hist.pipes.long["orders"]) != 0 || c.hist.pipes.acc["orders"] != nil || len(c.hist.tenants.acc) != 0 {
		t.Fatal("deleted pipeline still has rollups")
	}
}

func TestHistoryRollsUpBeyondTheFineTier(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	step, every := tuning.HistoryLongStep(), tuning.HistorySample()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	span := tuning.HistoryKeep() + 30*time.Minute
	for at := start; !at.After(start.Add(span)); at = at.Add(every) {
		c.hist.sample(c.d, at)
	}
	pts, spacing := c.hist.window("orders", start)
	if spacing != step {
		t.Fatalf("a window older than the fine tier used %s steps, want %s", spacing, step)
	}
	if want := int(span/step) - 1; len(pts) < want {
		t.Fatalf("long tier has %d points, want at least %d", len(pts), want)
	}
	for i := 1; i < len(pts); i++ {
		if d := pts[i].Time.Sub(pts[i-1].Time); d != step {
			t.Fatalf("rollups %d and %d are %s apart, want %s", i-1, i, d, step)
		}
	}
	recent, spacing := c.hist.window("orders", start.Add(span-10*time.Minute))
	if spacing != every || len(recent) != int(10*time.Minute/every)+1 {
		t.Fatalf("a recent window gave %d points every %s", len(recent), spacing)
	}
}

func TestHistoryRollupAveragesRatesAndSumsLatency(t *testing.T) {
	h := NewHistory().pipes
	base := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	for i, rate := range []float64{10, 20, 30} {
		h.roll("orders", Sample{Time: base.Add(time.Duration(i*5) * time.Second), Processed: rate, Lag: int64(i),
			Partitions: []PartitionSample{{Partition: 1, Lag: int64(i), PerSec: rate}, {Partition: 0, Lag: 5, PerSec: 1}}},
			map[float64]uint64{0.01: 1, 1: 1, math.Inf(1): 1})
	}
	h.roll("orders", Sample{Time: base.Add(time.Minute)}, map[float64]uint64{0.01: 0, 1: 99, math.Inf(1): 99})
	pts := h.long["orders"]
	if len(pts) != 1 {
		t.Fatalf("got %d rollups, want 1", len(pts))
	}
	p := pts[0]
	if p.Processed != 20 || p.Lag != 2 || !p.Time.Equal(base.Add(time.Minute)) {
		t.Fatalf("rollup = %+v", p)
	}
	if p.P99Ms <= 0 || p.P99Ms > 10 {
		t.Fatalf("p99 from three 10ms calls = %v", p.P99Ms)
	}
	if len(p.Partitions) != 2 || p.Partitions[0].Partition != 0 || p.Partitions[1].PerSec != 20 || p.Partitions[1].Lag != 2 {
		t.Fatalf("partitions = %+v", p.Partitions)
	}
}

func TestHistoryRecordsPartitionRatesFromCommits(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	now := time.Now()
	lags := func(c0, c1 int64) map[string][]kafkaadmin.PartitionLag {
		return map[string][]kafkaadmin.PartitionLag{"orders": {{Partition: 0, Committed: c0, End: 100, Lag: 100 - c0}, {Partition: 1, Committed: c1, End: 50, Lag: 50 - c1}}}
	}
	c.hist.sampleWith(c.d, now, lags(10, -1))
	c.hist.sampleWith(c.d, now.Add(10*time.Second), lags(60, 20))
	pts := c.hist.since("orders", time.Time{})
	if len(pts) != 1 || len(pts[0].Partitions) != 2 {
		t.Fatalf("points = %+v", pts)
	}
	if pts[0].Lag != 70 {
		t.Fatalf("pipeline lag = %d, want the partitions' 40+30", pts[0].Lag)
	}
	p0, p1 := pts[0].Partitions[0], pts[0].Partitions[1]
	if p0.PerSec != 5 || p0.Lag != 40 {
		t.Fatalf("partition 0 = %+v, want 5/s and lag 40", p0)
	}
	if p1.PerSec != 0 || p1.Lag != 30 {
		t.Fatalf("partition 1 had no earlier commit, got %+v", p1)
	}
}

func TestHistorySumsEachTenantsPipelines(t *testing.T) {
	withTenant := func(name, tenant string) config.Pipeline {
		return mustPipeline(t, pipelineYAML(name, name+".raw", name+".done")+"tenant: "+tenant+"\n")
	}
	src := &memSource{p: []config.Pipeline{withTenant("a", "acme"), withTenant("b", "acme"), withTenant("c", "globex")}}
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	now := time.Now()
	lags := func(a, b, cc int64) map[string][]kafkaadmin.PartitionLag {
		return map[string][]kafkaadmin.PartitionLag{"a": {{Lag: a}}, "b": {{Lag: b}}, "c": {{Lag: cc}}}
	}
	c.hist.sampleWith(c.d, now, lags(1, 2, 4))
	c.hist.sampleWith(c.d, now.Add(5*time.Second), lags(10, 20, 40))
	acme, _ := c.hist.tenantWindow("acme", time.Time{})
	globex, _ := c.hist.tenantWindow("globex", time.Time{})
	if len(acme) != 1 || acme[0].Lag != 30 || len(acme[0].Partitions) != 0 {
		t.Fatalf("acme = %+v, want one point with lag 10+20", acme)
	}
	if len(globex) != 1 || globex[0].Lag != 40 {
		t.Fatalf("globex = %+v", globex)
	}
	src.p = src.p[2:]
	c.hist.sampleWith(c.d, now.Add(10*time.Second), lags(0, 0, 0))
	if acme, _ := c.hist.tenantWindow("acme", time.Time{}); len(acme) != 0 {
		t.Fatalf("a tenant with no pipelines left still has %d points", len(acme))
	}
}
