package mcpserver

import (
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
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
	if n := len(c.hist.since("orders", time.Time{})); n != keep {
		t.Fatalf("kept %d points, want %d", n, keep)
	}
	src.p = nil
	c.hist.sample(c.d, now.Add(time.Hour*2))
	if n := len(c.hist.since("orders", time.Time{})); n != 0 {
		t.Fatalf("deleted pipeline still has %d points", n)
	}
}
