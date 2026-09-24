package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
	"github.com/raven-clown/ark/bridge-engine/internal/mcpserver"
	"github.com/raven-clown/ark/bridge-engine/internal/orchestrator"
)

// configReloader re-reads the config file and reconciles the running
// pipelines to match it via reconcile, which is orchestrator.Manager's own
// Reconcile in single-node mode, or cluster.Node's ApplyConfig (adjusting
// each pipeline's Workers to this node's placement share first) when
// cluster mode is on. It implements api.Reloader, so it also drives the
// manual POST /api/v1/config/reload endpoint.
type configReloader struct {
	path      string
	reconcile cluster.ReconcileFunc
	log       *slog.Logger
}

func (c *configReloader) Reload() error {
	cfg, err := config.Load(c.path)
	if err != nil {
		return err
	}
	if errs := c.reconcile(cfg.Pipelines); len(errs) > 0 {
		for _, e := range errs {
			c.log.Error("reconcile failed to start a pipeline", "error", e)
		}
		return errs[0]
	}
	return nil
}

// watchFile polls the config file's mtime and calls reload whenever it
// changes, so an operator (or a ConfigMap sync) editing the file on disk
// takes effect without a restart. A failed reload just logs and keeps the
// previously-running pipelines untouched.
func watchFile(ctx context.Context, path string, interval time.Duration, reload *configReloader, log *slog.Logger) {
	lastMod := time.Time{}
	if info, err := os.Stat(path); err == nil {
		lastMod = info.ModTime()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			if !info.ModTime().After(lastMod) {
				continue
			}
			lastMod = info.ModTime()
			log.Info("config file changed on disk, reloading", "path", path)
			if err := reload.Reload(); err != nil {
				log.Error("config reload failed, keeping previous pipelines running", "error", err)
			}
		}
	}
}

// clusterRegistry routes pause/resume through the cluster control topic,
// so a pause made through any node holds on every node and survives
// restarts. Everything else is served from this node's own Manager.
type clusterRegistry struct {
	*orchestrator.Manager
	node *cluster.Node
	log  *slog.Logger
}

func (c clusterRegistry) SetPaused(name string, paused bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.node.PublishPause(ctx, name, paused); err != nil {
		if !errors.Is(err, cluster.ErrUnknownPipeline) {
			c.log.Error("publishing cluster-wide pause failed", "pipeline", name, "error", err)
		}
		return false
	}
	return true
}

func localStats(mgr *orchestrator.Manager) map[string]cluster.PipelineStats {
	out := make(map[string]cluster.PipelineStats)
	for _, r := range mgr.Runners() {
		st := r.Status()
		agg := out[st.Pipeline]
		agg.Workers++
		agg.Processed += st.Processed
		agg.Rejected += st.Rejected
		agg.DeadLettered += st.DeadLettered
		agg.Failed += st.Failed
		agg.Paused = agg.Paused || st.Paused
		out[st.Pipeline] = agg
	}
	return out
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to pipelines config file")
	apiAddr := flag.String("api-addr", ":8080", "address for the REST API")
	flag.Parse()

	level := slog.LevelInfo
	if os.Getenv("BRIDGE_LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("loading config failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dlqState, err := dlq.NewStateStore(ctx, cfg.Brokers, cfg.Topics.ReplicationFactor, logger)
	if err != nil {
		logger.Error("starting dlq state store failed", "error", err)
		os.Exit(1)
	}
	go dlqState.Run(ctx)
	stateCtx, stateCancel := context.WithTimeout(ctx, 15*time.Second)
	dlqState.WaitCaughtUp(stateCtx)
	stateCancel()

	mgr := orchestrator.New(ctx, consumer.Deps{
		Brokers:           cfg.Brokers,
		ReplicationFactor: cfg.Topics.ReplicationFactor,
		DLQState:          dlqState,
	}, logger)

	reconcile := cluster.ReconcileFunc(mgr.Reconcile)
	var clusterNode *cluster.Node
	if cfg.Cluster.Enabled {
		clusterNode = cluster.New(cfg.Brokers, cfg.Cluster, cfg.Topics.ReplicationFactor, mgr.Reconcile, logger)
		clusterNode.SetStatsProvider(func() map[string]cluster.PipelineStats { return localStats(mgr) })
		clusterNode.SetPauseHandler(func(name string, paused bool) { mgr.RememberPause(name, paused) })
		clusterNode.SetConfigHandler(func(pipelines []config.Pipeline) {
			mgr.SyncStandbyBrowsers(pipelines)
			for _, err := range clusterNode.ApplyConfig(pipelines) {
				logger.Error("applying cluster pipeline config failed to start a pipeline", "error", err)
			}
		})
		if err := clusterNode.Start(ctx, cfg.Pipelines); err != nil {
			logger.Error("starting cluster node failed", "error", err)
			os.Exit(1)
		}
		// In cluster mode a reload publishes the file's pipelines to the
		// cluster; every node, this one included, applies them from there.
		reconcile = func(pipelines []config.Pipeline) []error {
			pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := clusterNode.PublishConfig(pubCtx, pipelines); err != nil {
				return []error{err}
			}
			return nil
		}
		logger.Info("cluster mode enabled", "cluster", cfg.Cluster.Name, "node_id", clusterNode.ID())
	}

	// In cluster mode Start already applied the cluster's config.
	if clusterNode == nil {
		if errs := reconcile(cfg.Pipelines); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("starting pipeline failed", "error", e)
			}
			os.Exit(1)
		}
	}

	redriveGate := func() bool { return true }
	if clusterNode != nil {
		redriveGate = clusterNode.IsLeader
	}
	go mgr.RunRedrive(ctx, redriveGate)

	reload := &configReloader{path: *configPath, reconcile: reconcile, log: logger}
	go watchFile(ctx, *configPath, 5*time.Second, reload, logger)

	rootMux := http.NewServeMux()
	apiTokens := authz.LoadFromEnv("ARK_API")
	if !apiTokens.Enabled() {
		logger.Warn("no ARK_API_*_TOKENS set: the REST API only accepts requests from localhost")
	}
	var registry api.Registry = mgr
	if clusterNode != nil {
		registry = clusterRegistry{Manager: mgr, node: clusterNode, log: logger}
	}
	rootMux.Handle("/", api.NewServer(registry, reload, clusterNode, apiTokens))

	mcpTokens := mcpserver.LoadTokenStoreFromEnv()
	if mcpTokens.Enabled() {
		auditLog := logger.With("component", "mcp-audit")
		rootMux.Handle("/mcp", mcpserver.NewHTTPHandler(registry, mcpTokens, auditLog))
		logger.Info("mcp server enabled", "path", "/mcp")
	} else {
		logger.Info("mcp server disabled: no ARK_MCP_*_TOKENS set")
	}

	apiServer := &http.Server{
		Addr:              *apiAddr,
		Handler:           rootMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		logger.Info("starting api server", "addr", *apiAddr)
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("api server stopped with error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("api server shutdown failed", "error", err)
	}
	<-serverDone

	mgr.ShutdownAll()
	logger.Info("bridge shut down")
}
