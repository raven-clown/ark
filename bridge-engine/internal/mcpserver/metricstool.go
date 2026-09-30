package mcpserver

import (
	"fmt"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

type metricsIn struct {
	Name      string `json:"name,omitempty" jsonschema:"a pipeline name; give this or tenant"`
	Tenant    string `json:"tenant,omitempty" jsonschema:"a tenant, for the sum of its pipelines; give this or name"`
	Minutes   int    `json:"minutes,omitempty" jsonschema:"how far back from now, default 60, at most history_long_keep_hours"`
	From      string `json:"from,omitempty" jsonschema:"start of the range as RFC 3339, instead of minutes"`
	To        string `json:"to,omitempty" jsonschema:"end of the range as RFC 3339, default now"`
	MaxPoints int    `json:"max_points,omitempty" jsonschema:"at most this many points, averaged down if needed, default 12; the summary always covers the whole range"`
}

type metricsSummary struct {
	AvgPerSec     float64 `json:"avg_msgs_per_sec"`
	PeakPerSec    float64 `json:"peak_msgs_per_sec"`
	Handled       int64   `json:"messages_handled_approx"`
	ErrorPct      float64 `json:"rejected_or_dead_lettered_pct"`
	LagNow        int64   `json:"lag_now"`
	LagPeak       int64   `json:"lag_peak"`
	P99PeakMs     float64 `json:"p99_peak_ms"`
	AvgCallbackMs float64 `json:"avg_callback_ms"`
}

type metricsOut struct {
	Of           string         `json:"of"`
	Units        string         `json:"units"`
	Timezone     string         `json:"timezone"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	EverySeconds int            `json:"every_seconds"`
	Summary      metricsSummary `json:"summary"`
	Points       []Sample       `json:"points"`
	Note         string         `json:"note,omitempty"`
}

func getMetrics(d Deps, in metricsIn) (metricsOut, error) {
	if d.History == nil {
		return metricsOut{}, fmt.Errorf("metrics history isn't kept on this endpoint")
	}
	if (in.Name == "") == (in.Tenant == "") {
		return metricsOut{}, fmt.Errorf("give either name (a pipeline) or tenant")
	}
	now := time.Now()
	to, from := now, time.Time{}
	var err error
	if in.To != "" {
		if to, err = time.Parse(time.RFC3339, in.To); err != nil {
			return metricsOut{}, fmt.Errorf("to must be RFC 3339, such as 2026-09-29T10:00:00Z: %w", err)
		}
	}
	oldest := now.Add(-tuning.HistoryLongKeep())
	switch {
	case in.From != "":
		if from, err = time.Parse(time.RFC3339, in.From); err != nil {
			return metricsOut{}, fmt.Errorf("from must be RFC 3339, such as 2026-09-29T09:00:00Z: %w", err)
		}
	case in.Minutes > 0:
		from = to.Add(-time.Duration(in.Minutes) * time.Minute)
	default:
		from = to.Add(-time.Hour)
	}
	if !from.Before(to) {
		return metricsOut{}, fmt.Errorf("from must be before to")
	}
	out := metricsOut{Timezone: d.loc().String(), Units: "*_per_sec are messages per second; *_ms and avg_callback_ms are milliseconds, not seconds; lag is messages"}
	if from.Before(oldest) {
		from = oldest
		out.Note = fmt.Sprintf("history only goes back %s, so the range starts there", tuning.HistoryLongKeep())
	}

	var pts []Sample
	var every time.Duration
	if in.Name != "" {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return metricsOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		out.Of = "pipeline " + p.Name
		pts, every = d.History.window(p.Name, from)
	} else {
		if err := tenantVisible(d, in.Tenant); err != nil {
			return metricsOut{}, err
		}
		out.Of = "tenant " + in.Tenant
		pts, every = d.History.tenantWindow(in.Tenant, from)
	}
	kept := pts[:0]
	for _, s := range pts {
		if !s.Time.After(to) {
			kept = append(kept, s)
		}
	}
	pts = kept

	out.Summary = summarize(pts, every)
	maxPoints := in.MaxPoints
	if maxPoints <= 0 {
		maxPoints = 12
	}
	pts, every = thin(pts, every, maxPoints)
	for i := range pts {
		pts[i].Time = d.localize(pts[i].Time)
	}
	out.From, out.To = d.localize(from).Format(time.RFC3339), d.localize(to).Format(time.RFC3339)
	out.EverySeconds, out.Points = int(every.Seconds()), pts
	if len(pts) == 0 && out.Note == "" {
		out.Note = "no samples in this range yet; ARK keeps history in memory, so it starts again after a restart"
	}
	return out, nil
}

// tenantVisible refuses a tenant whose sum would include pipelines the
// caller can't see.
func tenantVisible(d Deps, tenant string) error {
	visible := 0
	for _, p := range d.visiblePipelines() {
		if p.Tenant == tenant {
			visible++
		}
	}
	all := 0
	if d.Config != nil {
		for _, p := range d.Config.Pipelines() {
			if p.Tenant == tenant {
				all++
			}
		}
	}
	if visible == 0 {
		return fmt.Errorf("no visible pipelines belong to tenant %s", tenant)
	}
	if visible < all {
		return fmt.Errorf("tenant %s has pipelines this token can't see, so its total isn't available; ask per pipeline instead", tenant)
	}
	return nil
}

func summarize(pts []Sample, every time.Duration) metricsSummary {
	var s metricsSummary
	if len(pts) == 0 {
		return s
	}
	var sum, bad, ms float64
	var calls int
	for _, p := range pts {
		total := p.Processed + p.Rejected + p.DeadLettered
		sum += total
		bad += p.Rejected + p.DeadLettered
		s.PeakPerSec = max(s.PeakPerSec, total)
		s.LagPeak = max(s.LagPeak, p.Lag)
		s.P99PeakMs = max(s.P99PeakMs, p.P99Ms)
		if p.AvgCallbackMs > 0 {
			ms += p.AvgCallbackMs
			calls++
		}
	}
	s.AvgPerSec = sum / float64(len(pts))
	s.Handled = int64(sum * every.Seconds())
	if sum > 0 {
		s.ErrorPct = bad / sum * 100
	}
	if calls > 0 {
		s.AvgCallbackMs = ms / float64(calls)
	}
	s.LagNow = pts[len(pts)-1].Lag
	return s
}

// thin averages runs of points so at most n are left: rates and latencies
// are averaged, gauges keep the run's last value.
func thin(pts []Sample, every time.Duration, n int) ([]Sample, time.Duration) {
	if len(pts) <= n {
		return pts, every
	}
	k := (len(pts) + n - 1) / n
	out := make([]Sample, 0, n)
	for i := 0; i < len(pts); i += k {
		run := pts[i:min(i+k, len(pts))]
		m := run[len(run)-1]
		m.Partitions = nil
		var proc, rej, dl, fail, avg, p50, p95, p99 float64
		for _, p := range run {
			proc += p.Processed
			rej += p.Rejected
			dl += p.DeadLettered
			fail += p.Failed
			avg += p.AvgCallbackMs
			p50 += p.P50Ms
			p95 += p.P95Ms
			p99 += p.P99Ms
		}
		c := float64(len(run))
		m.Processed, m.Rejected, m.DeadLettered, m.Failed = proc/c, rej/c, dl/c, fail/c
		m.AvgCallbackMs, m.P50Ms, m.P95Ms, m.P99Ms = avg/c, p50/c, p95/c, p99/c
		out = append(out, m)
	}
	return out, every * time.Duration(k)
}
