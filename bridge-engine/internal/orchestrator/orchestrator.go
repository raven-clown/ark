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
	"github.com/raven-clown/ark/bridge-engine/internal/events"
)

type worker struct {
	runner *consumer.Runner
	cancel context.CancelFunc
	done   chan struct{}
}

type runningPipeline struct {
	cfg     config.Pipeline
	ctx     context.Context
	cancel  context.CancelFunc
	workers []*worker
	wg      sync.WaitGroup
}

func (rp *runningPipeline) runners() []*consumer.Runner {
	out := make([]*consumer.Runner, len(rp.workers))
	for i, w := range rp.workers {
		out[i] = w.runner
	}
	return out
}

type Manager struct {
	baseCtx context.Context
	deps    consumer.Deps
	logger  *slog.Logger

	reconcileMu sync.Mutex

	mu        sync.RWMutex
	pipelines map[string]*runningPipeline
	desired   map[string]config.Pipeline
	retrying  map[string]context.CancelFunc
	paused    map[string]bool
	standby   map[string]*standbyBrowsers
}

func New(baseCtx context.Context, deps consumer.Deps, logger *slog.Logger) *Manager {
	return &Manager{
		baseCtx:   baseCtx,
		deps:      deps,
		logger:    logger,
		pipelines: make(map[string]*runningPipeline),
		desired:   make(map[string]config.Pipeline),
		retrying:  make(map[string]context.CancelFunc),
		paused:    make(map[string]bool),
		standby:   make(map[string]*standbyBrowsers),
	}
}

func (m *Manager) Runners() []*consumer.Runner {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*consumer.Runner
	for _, rp := range m.pipelines {
		out = append(out, rp.runners()...)
	}
	return out
}

func (m *Manager) PipelineRunners(name string) []*consumer.Runner {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if rp, ok := m.pipelines[name]; ok {
		return rp.runners()
	}
	return nil
}

func (m *Manager) SetPaused(name string, paused bool) bool {
	m.mu.RLock()
	_, known := m.desired[name]
	m.mu.RUnlock()
	if !known {
		return false
	}
	return m.RememberPause(name, paused)
}

func (m *Manager) RememberPause(name string, paused bool) bool {
	m.mu.Lock()
	if paused {
		m.paused[name] = true
	} else {
		delete(m.paused, name)
	}
	rp, ok := m.pipelines[name]
	var runners []*consumer.Runner
	if ok {
		runners = rp.runners()
	}
	m.mu.Unlock()
	if !ok {
		return false
	}

	if paused {
		consumer.Pause(runners)
		events.Record(name, events.Paused, "pipeline paused by an operator; messages wait safely in source_topic", nil)
	} else {
		consumer.Resume(runners)
		events.Record(name, events.Resumed, "pipeline resumed by an operator", nil)
	}
	return true
}

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
		if ok && p.IsEnabled() && sameExceptWorkers(existing.cfg, p) {
			m.resize(p)
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
			events.Record(p.Name, events.PipelineStartFailed, "pipeline failed to start, retrying in the background: "+err.Error(), nil)
			m.retryStart(p)
		}
	}
	return errs
}

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

	runners, err := consumer.NewPipeline(pctx, m.deps, p, m.logger)
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

	rp := &runningPipeline{cfg: p, ctx: pctx, cancel: cancel}
	for _, runner := range runners {
		m.launch(rp, runner)
	}

	m.mu.Lock()
	m.pipelines[p.Name] = rp
	m.mu.Unlock()

	m.logger.Info("pipeline started", "pipeline", p.Name, "tenant", p.Tenant, "workers", len(runners))
	events.Record(p.Name, events.PipelineStarted, fmt.Sprintf("pipeline started with %d worker(s) on this node", len(runners)), nil)
	return nil
}

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
		events.Record(runner.Name(), events.WorkerRestarted, fmt.Sprintf("worker %d stopped on its own and is being restarted: %v", runner.WorkerID(), err), nil)
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
	if err := consumer.CloseShared(rp.runners()); err != nil {
		m.logger.Error("closing pipeline shared resources failed", "pipeline", name, "tenant", rp.cfg.Tenant, "error", err)
	}
	m.logger.Info("pipeline stopped", "pipeline", name, "tenant", rp.cfg.Tenant)
	events.Record(name, events.PipelineStopped, "pipeline stopped on this node (removed, disabled, or restarting with a changed config)", nil)
}

func (m *Manager) Restart(name string) (bool, error) {
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()

	m.mu.RLock()
	rp, ok := m.pipelines[name]
	m.mu.RUnlock()
	if !ok {
		return false, nil
	}
	cfg := rp.cfg
	events.Record(name, events.PipelineStopped, "pipeline restarting on this node at an operator's request", nil)
	m.stop(name)
	if err := m.start(cfg); err != nil {
		m.retryStart(cfg)
		return true, fmt.Errorf("stopped, but starting again failed (retrying in the background): %w", err)
	}
	return true, nil
}

func (m *Manager) launch(rp *runningPipeline, runner *consumer.Runner) {
	wctx, cancel := context.WithCancel(rp.ctx)
	w := &worker{runner: runner, cancel: cancel, done: make(chan struct{})}
	rp.workers = append(rp.workers, w)
	rp.wg.Add(1)
	go func() {
		defer rp.wg.Done()
		defer close(w.done)
		m.supervise(wctx, runner)
	}()
}

// sameExceptWorkers reports whether two configs differ only in Workers.
func sameExceptWorkers(a, b config.Pipeline) bool {
	a.Workers, b.Workers = 0, 0
	return reflect.DeepEqual(a, b)
}

func (m *Manager) resize(p config.Pipeline) {
	m.mu.Lock()
	rp := m.pipelines[p.Name]
	current := len(rp.workers)
	want := max(p.Workers, 1)
	var removed []*worker
	switch {
	case want > current:
		nextID := 0
		for _, w := range rp.workers {
			nextID = max(nextID, w.runner.WorkerID()+1)
		}
		for i := 0; i < want-current; i++ {
			m.launch(rp, consumer.AddRunner(rp.workers[0].runner, nextID+i, m.logger))
		}
	case want < current:
		removed = rp.workers[want:]
		rp.workers = rp.workers[:want]
	}
	rp.cfg = p
	m.mu.Unlock()

	for _, w := range removed {
		w.cancel()
	}
	for _, w := range removed {
		<-w.done
	}
	m.logger.Info("pipeline resized in place", "pipeline", p.Name, "tenant", p.Tenant, "from", current, "to", want)
	events.Record(p.Name, events.PipelineResized, fmt.Sprintf("workers on this node changed from %d to %d without a restart", current, want), nil)
}

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
