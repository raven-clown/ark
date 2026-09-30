package mcpserver

import (
	"context"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

type Sample struct {
	Time          time.Time         `json:"time"`
	Processed     float64           `json:"processed_per_sec"`
	Rejected      float64           `json:"rejected_per_sec"`
	DeadLettered  float64           `json:"dead_lettered_per_sec"`
	Failed        float64           `json:"failed_per_sec"`
	Lag           int64             `json:"lag"`
	AvgCallbackMs float64           `json:"avg_callback_ms"`
	P50Ms         float64           `json:"p50_ms"`
	P95Ms         float64           `json:"p95_ms"`
	P99Ms         float64           `json:"p99_ms"`
	Workers       int               `json:"workers"`
	Running       int               `json:"running"`
	Partitions    []PartitionSample `json:"partitions,omitempty"`
}

// PartitionSample is one source partition at the time of a sample.
type PartitionSample struct {
	Partition int     `json:"partition"`
	Lag       int64   `json:"lag"`
	PerSec    float64 `json:"committed_per_sec"`
}

// rollup gathers raw samples for one step of the long tier.
type rollup struct {
	start   time.Time
	n       int
	sum     Sample
	calls   int
	last    Sample
	buckets map[float64]uint64
	parts   map[int]*PartitionSample
	partN   map[int]int
}

// tiers holds series by name: raw samples, and rollups kept longer.
type tiers struct {
	data map[string][]Sample
	long map[string][]Sample
	acc  map[string]*rollup
}

func newTiers() tiers {
	return tiers{data: map[string][]Sample{}, long: map[string][]Sample{}, acc: map[string]*rollup{}}
}

type History struct {
	mu      sync.RWMutex
	pipes   tiers
	tenants tiers
	last    map[string]PipelineStats
	buckets map[string]map[float64]uint64
	commits map[string]map[int]int64
	at      time.Time
	gather  prometheus.Gatherer
	offsets func(ctx context.Context, group, topic string) ([]kafkaadmin.PartitionLag, error)
}

func NewHistory() *History {
	return &History{pipes: newTiers(), tenants: newTiers(), last: map[string]PipelineStats{},
		buckets: map[string]map[float64]uint64{}, commits: map[string]map[int]int64{}, gather: prometheus.DefaultGatherer}
}

func (c *Console) RunHistory(ctx context.Context) {
	if c.hist.offsets == nil && len(c.d.Brokers) > 0 {
		c.hist.offsets = func(ctx context.Context, group, topic string) ([]kafkaadmin.PartitionLag, error) {
			return kafkaadmin.GroupLag(ctx, c.d.Brokers, group, topic)
		}
	}
	t := time.NewTicker(tuning.HistorySample())
	defer t.Stop()
	for {
		c.hist.sampleWith(c.d, time.Now(), c.hist.partitionLags(ctx, c.d.visiblePipelines()))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (h *History) partitionLags(ctx context.Context, pipelines []config.Pipeline) map[string][]kafkaadmin.PartitionLag {
	if h.offsets == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, tuning.HistorySample())
	defer cancel()
	out := map[string][]kafkaadmin.PartitionLag{}
	for _, p := range pipelines {
		if p.ConsumerGroup == "" || p.SourceTopic == "" {
			continue
		}
		if lags, err := h.offsets(ctx, p.ConsumerGroup, p.SourceTopic); err == nil {
			out[p.Name] = lags
		}
	}
	return out
}

func (h *History) sample(d Deps, now time.Time) { h.sampleWith(d, now, nil) }

// tenantSum adds up one tenant's pipelines for one sample.
type tenantSum struct {
	pt      Sample
	ms, w   float64
	buckets map[float64]uint64
}

func (h *History) sampleWith(d Deps, now time.Time, lags map[string][]kafkaadmin.PartitionLag) {
	h.mu.Lock()
	defer h.mu.Unlock()
	dt := now.Sub(h.at).Seconds()
	seen, seenTenants := map[string]bool{}, map[string]bool{}
	sums := map[string]*tenantSum{}
	buckets := bucketCounts(h.gather)
	var views map[string]cluster.PipelineView
	if d.Cluster != nil {
		views = d.Cluster.ClusterPipelines()
	}
	for _, p := range d.visiblePipelines() {
		seen[p.Name] = true
		seenTenants[p.Tenant] = true
		s := *pipelineStats(p, pipelineStatuses(d.Registry, p.Name))
		if d.Cluster != nil {
			s = *clusterWide(&s, views[p.Name].Total)
			buckets[p.Name] = parseBuckets(views[p.Name].Total.LatencyBuckets)
		}
		prev, ok := h.last[p.Name]
		h.last[p.Name] = s
		prevBuckets := h.buckets[p.Name]
		h.buckets[p.Name] = buckets[p.Name]
		prevCommits := h.commits[p.Name]
		if l, found := lags[p.Name]; found {
			cur := map[int]int64{}
			for _, pl := range l {
				cur[pl.Partition] = pl.Committed
			}
			h.commits[p.Name] = cur
		}
		if !ok || h.at.IsZero() || dt <= 0 {
			continue
		}
		rate := func(cur, old int64) float64 {
			if cur < old { // workers restarted and counters reset
				return 0
			}
			return float64(cur-old) / dt
		}
		pt := Sample{
			Time:          now,
			Processed:     rate(s.Processed, prev.Processed),
			Rejected:      rate(s.Rejected, prev.Rejected),
			DeadLettered:  rate(s.DeadLettered, prev.DeadLettered),
			Failed:        rate(s.Failed, prev.Failed),
			Lag:           s.Lag,
			AvgCallbackMs: s.AvgCallbackMs,
			Workers:       s.LocalWorkers,
			Running:       s.Running,
		}
		if cur := buckets[p.Name]; cur != nil {
			pt.P50Ms, pt.P95Ms, pt.P99Ms = quantileMs(prevBuckets, cur, 0.5), quantileMs(prevBuckets, cur, 0.95), quantileMs(prevBuckets, cur, 0.99)
		}
		if l := lags[p.Name]; len(l) > 0 {
			pt.Lag = 0
			for _, pl := range l {
				pt.Lag += pl.Lag
			}
		}
		for _, pl := range lags[p.Name] {
			ps := PartitionSample{Partition: pl.Partition, Lag: pl.Lag}
			if old, had := prevCommits[pl.Partition]; had && old >= 0 && pl.Committed >= old {
				ps.PerSec = float64(pl.Committed-old) / dt
			}
			pt.Partitions = append(pt.Partitions, ps)
		}
		calls := delta(prevBuckets, buckets[p.Name])
		h.pipes.add(p.Name, pt, calls)

		t := sums[p.Tenant]
		if t == nil {
			t = &tenantSum{pt: Sample{Time: now}, buckets: map[float64]uint64{}}
			sums[p.Tenant] = t
		}
		t.pt.Processed += pt.Processed
		t.pt.Rejected += pt.Rejected
		t.pt.DeadLettered += pt.DeadLettered
		t.pt.Failed += pt.Failed
		t.pt.Lag += pt.Lag
		t.pt.Workers += pt.Workers
		t.pt.Running += pt.Running
		if handled := pt.Processed + pt.Rejected + pt.DeadLettered; pt.AvgCallbackMs > 0 && handled > 0 {
			t.ms += pt.AvgCallbackMs * handled
			t.w += handled
		}
		for b, n := range calls {
			t.buckets[b] += n
		}
	}
	for tenant, t := range sums {
		if t.w > 0 {
			t.pt.AvgCallbackMs = t.ms / t.w
		}
		t.pt.P50Ms, t.pt.P95Ms, t.pt.P99Ms = quantileMs(nil, t.buckets, 0.5), quantileMs(nil, t.buckets, 0.95), quantileMs(nil, t.buckets, 0.99)
		h.tenants.add(tenant, t.pt, t.buckets)
	}
	for name := range h.last {
		if !seen[name] {
			delete(h.last, name)
			delete(h.buckets, name)
			delete(h.commits, name)
		}
	}
	h.pipes.forget(seen)
	h.tenants.forget(seenTenants)
	h.at = now
}

func (t tiers) add(name string, pt Sample, calls map[float64]uint64) {
	series := append(t.data[name], pt)
	if keep := max(1, int(tuning.HistoryKeep()/tuning.HistorySample())); len(series) > keep {
		series = series[len(series)-keep:]
	}
	t.data[name] = series
	t.roll(name, pt, calls)
}

func (t tiers) forget(seen map[string]bool) {
	for name := range t.acc {
		if !seen[name] {
			delete(t.data, name)
			delete(t.long, name)
			delete(t.acc, name)
		}
	}
}

func delta(prev, cur map[float64]uint64) map[float64]uint64 {
	out := make(map[float64]uint64, len(cur))
	for b, c := range cur {
		if p := prev[b]; c >= p {
			out[b] = c - p
		} else {
			out[b] = c
		}
	}
	return out
}

func (t tiers) roll(name string, pt Sample, calls map[float64]uint64) {
	step := tuning.HistoryLongStep()
	start := pt.Time.Truncate(step)
	a := t.acc[name]
	if a != nil && !a.start.Equal(start) {
		t.long[name] = append(t.long[name], a.point(step))
		if keep := max(1, int(tuning.HistoryLongKeep()/step)); len(t.long[name]) > keep {
			t.long[name] = t.long[name][len(t.long[name])-keep:]
		}
		a = nil
	}
	if a == nil {
		a = &rollup{start: start, buckets: map[float64]uint64{}, parts: map[int]*PartitionSample{}, partN: map[int]int{}}
		t.acc[name] = a
	}
	a.n++
	a.sum.Processed += pt.Processed
	a.sum.Rejected += pt.Rejected
	a.sum.DeadLettered += pt.DeadLettered
	a.sum.Failed += pt.Failed
	if pt.AvgCallbackMs > 0 {
		a.sum.AvgCallbackMs += pt.AvgCallbackMs
		a.calls++
	}
	a.last = pt
	for b, n := range calls {
		a.buckets[b] += n
	}
	for _, ps := range pt.Partitions {
		agg := a.parts[ps.Partition]
		if agg == nil {
			agg = &PartitionSample{Partition: ps.Partition}
			a.parts[ps.Partition] = agg
		}
		agg.PerSec += ps.PerSec
		agg.Lag = ps.Lag
		a.partN[ps.Partition]++
	}
}

func (a *rollup) point(step time.Duration) Sample {
	n := float64(a.n)
	out := Sample{
		Time:         a.start.Add(step),
		Processed:    a.sum.Processed / n,
		Rejected:     a.sum.Rejected / n,
		DeadLettered: a.sum.DeadLettered / n,
		Failed:       a.sum.Failed / n,
		Lag:          a.last.Lag,
		Workers:      a.last.Workers,
		Running:      a.last.Running,
		P50Ms:        quantileMs(nil, a.buckets, 0.5),
		P95Ms:        quantileMs(nil, a.buckets, 0.95),
		P99Ms:        quantileMs(nil, a.buckets, 0.99),
	}
	if a.calls > 0 {
		out.AvgCallbackMs = a.sum.AvgCallbackMs / float64(a.calls)
	}
	ids := make([]int, 0, len(a.parts))
	for p := range a.parts {
		ids = append(ids, p)
	}
	sort.Ints(ids)
	for _, p := range ids {
		agg := a.parts[p]
		out.Partitions = append(out.Partitions, PartitionSample{Partition: p, Lag: agg.Lag, PerSec: agg.PerSec / float64(a.partN[p])})
	}
	return out
}

func (h *History) since(name string, from time.Time) []Sample {
	pts, _ := h.window(name, from)
	return pts
}

func (h *History) window(name string, from time.Time) ([]Sample, time.Duration) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.pipes.window(name, from, h.at)
}

// tenantWindow is window for the sum of a tenant's pipelines.
func (h *History) tenantWindow(tenant string, from time.Time) ([]Sample, time.Duration) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.tenants.window(tenant, from, h.at)
}

func (t tiers) window(name string, from, at time.Time) ([]Sample, time.Duration) {
	src, every := t.data[name], tuning.HistorySample()
	if at.Sub(from) > tuning.HistoryKeep()+every && len(t.long[name]) > 0 {
		src, every = t.long[name], tuning.HistoryLongStep()
	}
	out := []Sample{}
	for _, s := range src {
		if !s.Time.Before(from) {
			out = append(out, s)
		}
	}
	return out, every
}

func (c *Console) historyRoute(w http.ResponseWriter, r *http.Request) {
	limit := int(tuning.HistoryLongKeep().Minutes())
	minutes, err := strconv.Atoi(r.URL.Query().Get("minutes"))
	if err != nil || minutes <= 0 || minutes > limit {
		minutes = 15
	}
	from := time.Now().Add(-time.Duration(minutes) * time.Minute)
	out := map[string]any{"timezone": c.d.loc().String(), "every_seconds": int(tuning.HistorySample().Seconds()), "max_minutes": limit,
		"fine_minutes": int(tuning.HistoryKeep().Minutes())}
	localize := func(pts []Sample, every time.Duration) []Sample {
		out["every_seconds"] = int(every.Seconds())
		for i := range pts {
			pts[i].Time = c.d.localize(pts[i].Time)
		}
		return pts
	}
	q := r.URL.Query()
	series, tenants := map[string][]Sample{}, map[string][]Sample{}
	for _, p := range c.d.visiblePipelines() {
		if want := q.Get("pipeline"); (want != "" && want != p.Name) || q.Has("tenant") {
			continue
		}
		series[p.Name] = localize(c.hist.window(p.Name, from))
	}
	if q.Has("tenant") {
		tenants[q.Get("tenant")] = localize(c.hist.tenantWindow(q.Get("tenant"), from))
	}
	out["pipelines"], out["tenants"] = series, tenants
	writeJSON(w, http.StatusOK, out)
}

var started = time.Now()

// nodeRoute describes this ARK process, for the console's instance strip.
func (c *Console) nodeRoute(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	out := map[string]any{
		"version":          c.d.Version,
		"go_version":       runtime.Version(),
		"uptime_seconds":   int64(time.Since(started).Seconds()),
		"goroutines":       runtime.NumGoroutine(),
		"heap_alloc_bytes": m.HeapAlloc,
		"sys_bytes":        m.Sys,
		"num_cpu":          runtime.NumCPU(),
		"gomaxprocs":       runtime.GOMAXPROCS(0),
		"gc_cycles":        m.NumGC,
		"timezone":         c.d.loc().String(),
		"started_at":       c.d.localize(started).Format(time.RFC3339),
	}
	if c.d.Cluster != nil {
		st := c.d.Cluster.StatusSnapshot()
		out["node_id"], out["cluster"], out["leader"], out["live_nodes"] = st.NodeID, st.Cluster, st.Leader, len(st.LiveNodes)
	}
	writeJSON(w, http.StatusOK, out)
}
