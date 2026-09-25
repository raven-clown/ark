package mcpserver

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
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
}

type history struct {
	mu   sync.RWMutex
	data map[string][]Sample
	last map[string]PipelineStats
	at   time.Time
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
	for _, p := range d.visiblePipelines() {
		seen[p.Name] = true
		s := *pipelineStats(p, pipelineStatuses(d.Registry, p.Name))
		prev, ok := h.last[p.Name]
		h.last[p.Name] = s
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
