package mcpserver

import (
	"context"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	historyEvery = 5 * time.Second
	historyKeep  = 720 // one hour at historyEvery
)

// Sample is one point of a pipeline's history on this node: rates since
// the previous sample, and gauges at the time of the sample.
type Sample struct {
	Time          time.Time `json:"time"`
	Processed     float64   `json:"processed_per_sec"`
	Rejected      float64   `json:"rejected_per_sec"`
	DeadLettered  float64   `json:"dead_lettered_per_sec"`
	Failed        float64   `json:"failed_per_sec"`
	Lag           int64     `json:"lag"`
	AvgCallbackMs float64   `json:"avg_callback_ms"`
	// Callback latency percentiles over the interval, from the
	// ark_callback_duration_seconds histogram; 0 when no calls were made.
	P50Ms   float64 `json:"p50_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	Workers int     `json:"workers"`
	Running int     `json:"running"`
}

type history struct {
	mu      sync.RWMutex
	data    map[string][]Sample
	last    map[string]PipelineStats
	buckets map[string]map[float64]uint64
	at      time.Time
	gather  prometheus.Gatherer
}

func newHistory() *history {
	return &history{data: map[string][]Sample{}, last: map[string]PipelineStats{}, buckets: map[string]map[float64]uint64{}, gather: prometheus.DefaultGatherer}
}

// RunHistory samples every pipeline's numbers until ctx ends, so the
// console can chart the last hour as soon as it opens.
func (c *Console) RunHistory(ctx context.Context) {
	t := time.NewTicker(historyEvery)
	defer t.Stop()
	for {
		c.hist.sample(c.d, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (h *history) sample(d Deps, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	dt := now.Sub(h.at).Seconds()
	seen := map[string]bool{}
	buckets := bucketCounts(h.gather)
	for _, p := range d.visiblePipelines() {
		seen[p.Name] = true
		s := *pipelineStats(p, pipelineStatuses(d.Registry, p.Name))
		prev, ok := h.last[p.Name]
		h.last[p.Name] = s
		prevBuckets := h.buckets[p.Name]
		h.buckets[p.Name] = buckets[p.Name]
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
		series := append(h.data[p.Name], pt)
		if len(series) > historyKeep {
			series = series[len(series)-historyKeep:]
		}
		h.data[p.Name] = series
	}
	for name := range h.data {
		if !seen[name] {
			delete(h.data, name)
			delete(h.last, name)
			delete(h.buckets, name)
		}
	}
	h.at = now
}

func (h *history) since(name string, from time.Time) []Sample {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := []Sample{}
	for _, s := range h.data[name] {
		if !s.Time.Before(from) {
			out = append(out, s)
		}
	}
	return out
}

func (c *Console) historyRoute(w http.ResponseWriter, r *http.Request) {
	minutes, err := strconv.Atoi(r.URL.Query().Get("minutes"))
	if err != nil || minutes <= 0 || minutes > 60 {
		minutes = 15
	}
	from := time.Now().Add(-time.Duration(minutes) * time.Minute)
	out := map[string]any{"timezone": c.d.loc().String(), "every_seconds": int(historyEvery.Seconds())}
	series := map[string][]Sample{}
	for _, p := range c.d.visiblePipelines() {
		if want := r.URL.Query().Get("pipeline"); want != "" && want != p.Name {
			continue
		}
		pts := c.hist.since(p.Name, from)
		for i := range pts {
			pts[i].Time = c.d.localize(pts[i].Time)
		}
		series[p.Name] = pts
	}
	out["pipelines"] = series
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
