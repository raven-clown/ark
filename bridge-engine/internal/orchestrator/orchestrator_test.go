package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/consumer"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestSameExceptWorkers(t *testing.T) {
	a := config.Pipeline{Name: "p", SourceTopic: "s", Workers: 2}
	b := a
	b.Workers = 5
	if !sameExceptWorkers(a, b) {
		t.Error("configs differing only in Workers should resize in place")
	}
	b.SourceTopic = "other"
	if sameExceptWorkers(a, b) {
		t.Error("a different source topic must restart the pipeline")
	}
}

func TestNewInitializesAllMaps(t *testing.T) {
	m := New(context.Background(), consumer.Deps{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if m.pipelines == nil || m.desired == nil || m.retrying == nil || m.paused == nil || m.standby == nil {
		t.Fatal("every Manager map must be initialized by New")
	}
	m.SyncStandbyBrowsers(nil)
}
