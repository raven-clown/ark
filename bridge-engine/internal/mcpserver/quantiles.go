package mcpserver

import (
	"math"
	"sort"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

func bucketCounts(g prometheus.Gatherer) map[string]map[float64]uint64 {
	out := map[string]map[float64]uint64{}
	families, err := g.Gather()
	if err != nil {
		return out
	}
	for _, f := range families {
		if f.GetName() != "ark_callback_duration_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			pipeline := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "pipeline" {
					pipeline = l.GetValue()
				}
			}
			h := m.GetHistogram()
			if h == nil {
				continue
			}
			b := out[pipeline]
			if b == nil {
				b = map[float64]uint64{}
				out[pipeline] = b
			}
			for _, bk := range h.GetBucket() {
				b[bk.GetUpperBound()] += bk.GetCumulativeCount()
			}
			b[math.Inf(1)] += h.GetSampleCount()
		}
	}
	return out
}

func LatencyBuckets() map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	for pipeline, buckets := range bucketCounts(prometheus.DefaultGatherer) {
		m := make(map[string]uint64, len(buckets))
		for le, n := range buckets {
			m[strconv.FormatFloat(le, 'g', -1, 64)] = n
		}
		out[pipeline] = m
	}
	return out
}

// parseBuckets turns LatencyBuckets keys back into upper bounds.
func parseBuckets(in map[string]uint64) map[float64]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[float64]uint64, len(in))
	for le, n := range in {
		if f, err := strconv.ParseFloat(le, 64); err == nil {
			out[f] = n
		}
	}
	return out
}

func quantileMs(prev, cur map[float64]uint64, q float64) float64 {
	bounds := make([]float64, 0, len(cur))
	for b := range cur {
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)
	delta := func(b float64) float64 {
		c, p := cur[b], prev[b]
		if c < p {
			return float64(c)
		}
		return float64(c - p)
	}
	total := delta(math.Inf(1))
	if total <= 0 {
		return 0
	}
	rank := q * total
	lower, below := 0.0, 0.0
	for _, b := range bounds {
		n := delta(b)
		if n >= rank {
			if math.IsInf(b, 1) {
				return lower * 1000
			}
			inBucket := n - below
			if inBucket <= 0 {
				return b * 1000
			}
			return (lower + (b-lower)*(rank-below)/inBucket) * 1000
		}
		lower, below = b, n
	}
	return lower * 1000
}
