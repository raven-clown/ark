package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
)

type runningPipeline struct {
	cfg     config.Pipeline
	cancel  context.CancelFunc
	runners []*consumer.Runner
	wg      sync.WaitGroup
}

// Manager owns the set of currently-running pipelines and implements
// api.Registry, so the REST/MCP layer always sees whatever Reconcile last
// converged to, including across a config reload.
//
// Every pipeline Manager starts runs under baseCtx, not whatever caller
// happened to trigger the start. Reconcile is called from the HTTP
// reload handler with that request's context, which is cancelled the
// moment the response is written; a pipeline started under it would be
// killed within milliseconds. baseCtx is the process's own lifetime
// instead, so a pipeline started by a reload keeps running exactly like
// one started at boot, until the process shuts down or a later Reconcile
// stops it.
type Manager struct {
	baseCtx context.Context
	brokers []string
	logger  *slog.Logger

	mu        sync.RWMutex
	pipelines map[string]*runningPipeline
}

func New(baseCtx context.Context, brokers []string, logger *slog.Logger) *Manager {
	return &Manager{
		baseCtx:   baseCtx,
		brokers:   brokers,
		logger:    logger,
		pipelines: make(map[string]*runningPipeline),
	}
}

func (m *Manager) Runners() []*consumer.Runner {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*consumer.Runner
	for _, rp := range m.pipelines {
		out = append(out, rp.runners...)
	}
	return out
}

func (m *Manager) PipelineRunners(name string) []*consumer.Runner {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if rp, ok := m.pipelines[name]; ok {
		return rp.runners
	}
	return nil
}

// Reconcile starts, stops, or restarts pipelines so the running set matches
// pipelines exactly: pipelines no longer present are stopped, new ones are
// started, and ones whose config changed are restarted (stopped, then
// started fresh) since fields like workers, topics, or consumer_group
// aren't safe to change on a live Runner. Unchanged pipelines are left
// running untouched. Errors starting individual pipelines are collected and
// returned together; a failure in one pipeline doesn't stop the rest of the
// reconcile.
func (m *Manager) Reconcile(pipelines []config.Pipeline) []error {
	desired := make(map[string]config.Pipeline, len(pipelines))
	for _, p := range pipelines {
		desired[p.Name] = p
	}

	m.mu.RLock()
	var toStop []string
	for name := range m.pipelines {
		if _, ok := desired[name]; !ok {
			toStop = append(toStop, name)
		}
	}
	m.mu.RUnlock()

	for _, name := range toStop {
		m.stop(name)
	}

	var errs []error
	for _, p := range pipelines {
		m.mu.RLock()
		existing, ok := m.pipelines[p.Name]
		m.mu.RUnlock()

		if ok && reflect.DeepEqual(existing.cfg, p) {
			continue
		}
		if ok {
			m.stop(p.Name)
		}
		if !p.IsEnabled() {
			continue
		}
		if err := m.start(p); err != nil {
			errs = append(errs, fmt.Errorf("pipeline %q: %w", p.Name, err))
		}
	}
	return errs
}

func (m *Manager) start(p config.Pipeline) error {
	pctx, cancel := context.WithCancel(m.baseCtx)

	runners, err := consumer.NewPipeline(pctx, m.brokers, p, m.logger)
	if err != nil {
		cancel()
		return err
	}

	rp := &runningPipeline{cfg: p, cancel: cancel, runners: runners}
	rp.wg.Add(1)
	go func() {
		defer rp.wg.Done()

		var workers sync.WaitGroup
		for _, runner := range runners {
			workers.Add(1)
			go func(runner *consumer.Runner) {
				defer workers.Done()
				m.logger.Info("starting pipeline worker", "pipeline", runner.Name(), "tenant", runner.Tenant())
				if err := runner.Run(pctx); err != nil {
					m.logger.Error("pipeline worker stopped with error", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err)
				}
				if err := runner.Close(); err != nil {
					m.logger.Error("closing worker resources failed", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err)
				}
			}(runner)
		}
		workers.Wait()

		if err := consumer.CloseShared(runners); err != nil {
			m.logger.Error("closing pipeline shared resources failed", "pipeline", p.Name, "tenant", p.Tenant, "error", err)
		}
	}()

	m.mu.Lock()
	m.pipelines[p.Name] = rp
	m.mu.Unlock()

	m.logger.Info("pipeline started", "pipeline", p.Name, "tenant", p.Tenant)
	return nil
}

func (m *Manager) stop(name string) {
	m.mu.Lock()
	rp, ok := m.pipelines[name]
	if ok {
		delete(m.pipelines, name)
	}
	m.mu.Unlock()

	if !ok {
		return
	}

	rp.cancel()
	rp.wg.Wait()
	m.logger.Info("pipeline stopped", "pipeline", name, "tenant", rp.cfg.Tenant)
}

// ShutdownAll stops every running pipeline and waits for them to finish
// closing their Kafka resources. Call it once, during process shutdown.
func (m *Manager) ShutdownAll() {
	m.mu.RLock()
	names := make([]string, 0, len(m.pipelines))
	for name := range m.pipelines {
		names = append(names, name)
	}
	m.mu.RUnlock()

	for _, name := range names {
		m.stop(name)
	}
}
