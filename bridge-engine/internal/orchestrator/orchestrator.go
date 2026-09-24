package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

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
// killed within milliseconds.
type Manager struct {
	baseCtx           context.Context
	brokers           []string
	replicationFactor int
	logger            *slog.Logger

	// reconcileMu serializes whole Reconcile calls. The file watcher, the
	// reload endpoint and cluster placement can all trigger one, and two
	// interleaved starts of the same pipeline would orphan a full set of
	// consumers that nothing could stop.
	reconcileMu sync.Mutex

	mu        sync.RWMutex
	pipelines map[string]*runningPipeline
	desired   map[string]config.Pipeline
	retrying  map[string]context.CancelFunc
	paused    map[string]bool
}

func New(baseCtx context.Context, brokers []string, replicationFactor int, logger *slog.Logger) *Manager {
	return &Manager{
		baseCtx:           baseCtx,
		brokers:           brokers,
		replicationFactor: replicationFactor,
		logger:            logger,
		pipelines:         make(map[string]*runningPipeline),
		desired:           make(map[string]config.Pipeline),
		retrying:          make(map[string]context.CancelFunc),
		paused:            make(map[string]bool),
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

// SetPaused pauses or resumes a pipeline and remembers the choice, so a
// restart caused by a reload or a cluster placement change doesn't
// silently resume a pipeline an operator paused on purpose. It reports
// false if no pipeline by that name is running here.
func (m *Manager) SetPaused(name string, paused bool) bool {
	m.mu.Lock()
	rp, ok := m.pipelines[name]
	if ok {
		if paused {
			m.paused[name] = true
		} else {
			delete(m.paused, name)
		}
	}
	m.mu.Unlock()
	if !ok {
		return false
	}

	if paused {
		consumer.Pause(rp.runners)
	} else {
		consumer.Resume(rp.runners)
	}
	return true
}

// Reconcile starts, stops, or restarts pipelines so the running set matches
// pipelines exactly: pipelines no longer present are stopped, new ones are
// started, and ones whose config changed are restarted, since fields like
// workers, topics, or consumer_group aren't safe to change on a live
// Runner. Unchanged pipelines are left running untouched.
//
// A pipeline that fails to start (for example Kafka briefly unreachable)
// is not left down: it keeps retrying in the background with backoff until
// it starts or a later Reconcile changes or removes it. The failure is
// still returned so the caller can report it.
func (m *Manager) Reconcile(pipelines []config.Pipeline) []error {
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()

	desired := make(map[string]config.Pipeline, len(pipelines))
	for _, p := range pipelines {
		desired[p.Name] = p
	}

	m.mu.Lock()
	m.desired = desired
	for name, cancel := range m.retrying {
		if want, ok := desired[name]; !ok || !want.IsEnabled() {
			cancel()
			delete(m.retrying, name)
		}
	}
	var toStop []string
	for name := range m.pipelines {
		if _, ok := desired[name]; !ok {
			toStop = append(toStop, name)
		}
	}
	m.mu.Unlock()

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
			m.retryStart(p)
		}
	}
	return errs
}

// retryStart keeps trying to start p until it succeeds, the process shuts
// down, or a later Reconcile no longer wants this exact config.
func (m *Manager) retryStart(p config.Pipeline) {
	ctx, cancel := context.WithCancel(m.baseCtx)

	m.mu.Lock()
	if old, ok := m.retrying[p.Name]; ok {
		old()
	}
	m.retrying[p.Name] = cancel
	m.mu.Unlock()

	go func() {
		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			m.reconcileMu.Lock()
			m.mu.RLock()
			want, stillWanted := m.desired[p.Name]
			_, running := m.pipelines[p.Name]
			m.mu.RUnlock()
			if ctx.Err() != nil || !stillWanted || !reflect.DeepEqual(want, p) || running {
				m.reconcileMu.Unlock()
				return
			}
			err := m.start(p)
			m.reconcileMu.Unlock()

			if err == nil {
				m.logger.Info("pipeline started after retrying", "pipeline", p.Name, "tenant", p.Tenant)
				m.mu.Lock()
				delete(m.retrying, p.Name)
				m.mu.Unlock()
				return
			}
			m.logger.Error("pipeline still failing to start, will retry", "pipeline", p.Name, "tenant", p.Tenant, "error", err, "retry_in", backoff.String())
			backoff = min(backoff*2, 30*time.Second)
		}
	}()
}

func (m *Manager) start(p config.Pipeline) error {
	pctx, cancel := context.WithCancel(m.baseCtx)

	runners, err := consumer.NewPipeline(pctx, m.brokers, p, m.replicationFactor, m.logger)
	if err != nil {
		cancel()
		return err
	}

	m.mu.RLock()
	paused := m.paused[p.Name]
	m.mu.RUnlock()
	if paused {
		consumer.Pause(runners)
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
				m.supervise(pctx, runner)
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

	m.logger.Info("pipeline started", "pipeline", p.Name, "tenant", p.Tenant, "workers", len(runners))
	return nil
}

// supervise runs one worker and restarts it with backoff if it stops on its
// own, so a transient fetch error doesn't leave the pipeline permanently
// short a worker. It returns once ctx is cancelled.
func (m *Manager) supervise(ctx context.Context, runner *consumer.Runner) {
	defer func() {
		if err := runner.Close(); err != nil {
			m.logger.Error("closing worker resources failed", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err)
		}
	}()

	backoff := time.Second
	for {
		m.logger.Info("starting pipeline worker", "pipeline", runner.Name(), "tenant", runner.Tenant())
		err := runner.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		m.logger.Error("pipeline worker stopped, restarting it", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err, "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
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
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()

	m.mu.Lock()
	for name, cancel := range m.retrying {
		cancel()
		delete(m.retrying, name)
	}
	names := make([]string, 0, len(m.pipelines))
	for name := range m.pipelines {
		names = append(names, name)
	}
	m.mu.Unlock()

	for _, name := range names {
		m.stop(name)
	}
}
