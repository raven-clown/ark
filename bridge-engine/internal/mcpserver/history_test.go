package mcpserver

import (
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestHistoryKeepsAnHourAndForgetsDeletedPipelines(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	now := time.Now()
	c.hist.sample(c.d, now)
	if n := len(c.hist.since("orders", time.Time{})); n != 0 {
		t.Fatalf("the first sample is only a baseline, got %d points", n)
	}
	for i := 1; i <= historyKeep+10; i++ {
		c.hist.sample(c.d, now.Add(time.Duration(i)*historyEvery))
	}
	if n := len(c.hist.since("orders", time.Time{})); n != historyKeep {
		t.Fatalf("kept %d points, want %d", n, historyKeep)
	}
	src.p = nil
	c.hist.sample(c.d, now.Add(time.Hour*2))
	if n := len(c.hist.since("orders", time.Time{})); n != 0 {
		t.Fatalf("deleted pipeline still has %d points", n)
	}
}
