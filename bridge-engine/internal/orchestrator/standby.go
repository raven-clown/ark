package orchestrator

import (
	"context"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

const standbyMaxEntries = 200

// standbyBrowsers lets a node inspect, retry and discard dead-letter and
// reject entries of a pipeline it isn't running itself, so in a cluster any
// node can answer for any pipeline.
type standbyBrowsers struct {
	cfg    config.Pipeline
	dlq    *dlq.Browser
	reject *dlq.Browser
	source *producer.Producer
	cancel context.CancelFunc
}

// SyncStandbyBrowsers keeps one standby DLQ/reject browser per configured
// pipeline that has such topics, independent of whether it runs here.
func (m *Manager) SyncStandbyBrowsers(pipelines []config.Pipeline) {
	want := make(map[string]config.Pipeline)
	for _, p := range pipelines {
		if p.DeadLetterTopic != "" || p.RejectTopic != "" {
			want[p.Name] = p
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for name, sb := range m.standby {
		if p, ok := want[name]; !ok || p.DeadLetterTopic != sb.cfg.DeadLetterTopic || p.RejectTopic != sb.cfg.RejectTopic || p.SourceTopic != sb.cfg.SourceTopic {
			sb.cancel()
			_ = sb.source.Close()
			delete(m.standby, name)
		} else {
			sb.cfg = p
		}
	}
	for name, p := range want {
		if _, ok := m.standby[name]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(m.baseCtx)
		log := m.logger.With("pipeline", p.Name, "tenant", p.Tenant, "standby", true)
		sb := &standbyBrowsers{cfg: p, source: producer.New(m.deps.Brokers, p.SourceTopic), cancel: cancel}
		if p.DeadLetterTopic != "" {
			sb.dlq = dlq.NewBrowser(m.deps.Brokers, p.DeadLetterTopic, m.deps.DLQState, standbyMaxEntries, sb.source, log)
			go sb.dlq.Run(ctx)
		}
		if p.RejectTopic != "" {
			sb.reject = dlq.NewBrowser(m.deps.Brokers, p.RejectTopic, m.deps.DLQState, standbyMaxEntries, sb.source, log)
			go sb.reject.Run(ctx)
		}
		m.standby[name] = sb
	}
}

// DLQBrowser returns the standby browser for a pipeline's "dlq" or "reject"
// topic and the pipeline's mcp_access, if one exists.
func (m *Manager) DLQBrowser(name, kind string) (*dlq.Browser, config.MCPAccess, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sb, ok := m.standby[name]
	if !ok {
		return nil, "", false
	}
	var b *dlq.Browser
	switch kind {
	case "dlq":
		b = sb.dlq
	case "reject":
		b = sb.reject
	}
	return b, sb.cfg.MCPAccess, b != nil
}
