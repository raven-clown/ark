package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/mcpserver"
)

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

	var allRunners []*consumer.Runner
	var pipelineGroups [][]*consumer.Runner
	for _, p := range cfg.Pipelines {
		if !p.IsEnabled() {
			logger.Info("skipping disabled pipeline", "pipeline", p.Name, "tenant", p.Tenant)
			continue
		}
		group, err := consumer.NewPipeline(ctx, cfg.Brokers, p, logger)
		if err != nil {
			logger.Error("starting pipeline failed", "pipeline", p.Name, "tenant", p.Tenant, "error", err)
			os.Exit(1)
		}
		pipelineGroups = append(pipelineGroups, group)
		allRunners = append(allRunners, group...)
	}

	reg := api.NewRegistry(allRunners)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", api.NewServer(reg))

	mcpTokens := mcpserver.LoadTokenStoreFromEnv()
	if mcpTokens.Enabled() {
		auditLog := logger.With("component", "mcp-audit")
		rootMux.Handle("/mcp", mcpserver.NewHTTPHandler(reg, mcpTokens, auditLog))
		logger.Info("mcp server enabled", "path", "/mcp")
	} else {
		logger.Info("mcp server disabled: no ARK_MCP_*_TOKENS set")
	}

	apiServer := &http.Server{
		Addr:              *apiAddr,
		Handler:           rootMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("starting api server", "addr", *apiAddr)
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("api server stopped with error", "error", err)
		}
	}()

	for _, group := range pipelineGroups {
		wg.Add(1)
		go func(group []*consumer.Runner) {
			defer wg.Done()

			var pwg sync.WaitGroup
			for _, runner := range group {
				pwg.Add(1)
				go func(runner *consumer.Runner) {
					defer pwg.Done()
					logger.Info("starting pipeline worker", "pipeline", runner.Name(), "tenant", runner.Tenant())
					if err := runner.Run(ctx); err != nil {
						logger.Error("pipeline worker stopped with error", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err)
					}
					if err := runner.Close(); err != nil {
						logger.Error("closing worker resources failed", "pipeline", runner.Name(), "tenant", runner.Tenant(), "error", err)
					}
				}(runner)
			}
			pwg.Wait()

			if err := consumer.CloseShared(group); err != nil {
				logger.Error("closing pipeline shared resources failed", "error", err)
			}
		}(group)
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("api server shutdown failed", "error", err)
	}

	wg.Wait()
	logger.Info("bridge shut down")
}
