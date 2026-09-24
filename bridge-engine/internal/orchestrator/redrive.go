package orchestrator

import (
	"context"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
)

const redriveInterval = 10 * time.Second

// RunRedrive resends dead-lettered messages of pipelines that configure
// dead_letter_redrive, once each is old enough and hasn't been redriven
// max_times already. shouldRun gates it, so in a cluster only the leader
// redrives; the shared DLQ state store makes each entry go out only once.
func (m *Manager) RunRedrive(ctx context.Context, shouldRun func() bool) {
	ticker := time.NewTicker(redriveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !shouldRun() {
			continue
		}
		m.redriveOnce(ctx)
	}
}

func (m *Manager) redriveOnce(ctx context.Context) {
	m.mu.RLock()
	type target struct {
		name    string
		browser *dlq.Browser
		after   time.Duration
		max     int
	}
	var targets []target
	for name, p := range m.desired {
		if !p.IsEnabled() || !p.DeadLetterRedrive.Enabled() {
			continue
		}
		var b *dlq.Browser
		if rp, ok := m.pipelines[name]; ok && len(rp.workers) > 0 {
			b = rp.workers[0].runner.DLQBrowser()
		}
		if b == nil {
			if sb, ok := m.standby[name]; ok {
				b = sb.dlq
			}
		}
		if b != nil {
			targets = append(targets, target{name, b, time.Duration(p.DeadLetterRedrive.AfterSeconds) * time.Second, p.DeadLetterRedrive.MaxTimes})
		}
	}
	m.mu.RUnlock()

	for _, t := range targets {
		for _, e := range t.browser.List() {
			if e.Redrives >= t.max || time.Since(e.Timestamp) < t.after {
				continue
			}
			if err := t.browser.Retry(ctx, e.ID); err != nil {
				m.logger.Error("redriving dead-lettered message failed", "pipeline", t.name, "id", e.ID, "error", err)
				continue
			}
			m.logger.Info("redrove dead-lettered message", "pipeline", t.name, "id", e.ID, "redrive", e.Redrives+1, "max", t.max)
		}
	}
}
