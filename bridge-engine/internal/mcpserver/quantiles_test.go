package mcpserver

import (
	"math"
	"testing"
)

func TestQuantileFromBucketDeltas(t *testing.T) {
	inf := math.Inf(1)
	prev := map[float64]uint64{0.001: 10, 0.005: 10, 0.01: 10, inf: 10}
	// 100 new calls: 50 under 1ms, 40 between 1 and 5ms, 10 between 5 and 10ms.
	cur := map[float64]uint64{0.001: 60, 0.005: 100, 0.01: 110, inf: 110}
	if p := quantileMs(prev, cur, 0.5); math.Abs(p-1) > 0.01 {
		t.Errorf("p50 = %v, want 1ms", p)
	}
	if p := quantileMs(prev, cur, 0.95); p <= 5 || p > 10 {
		t.Errorf("p95 = %v, want between 5 and 10ms", p)
	}
	if p := quantileMs(cur, cur, 0.5); p != 0 {
		t.Errorf("no calls should give 0, got %v", p)
	}
}
