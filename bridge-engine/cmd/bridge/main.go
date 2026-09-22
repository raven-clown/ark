package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to pipelines config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("loading config failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for _, p := range cfg.Pipelines {
		if !p.IsEnabled() {
			logger.Info("skipping disabled pipeline", "pipeline", p.Name)
			continue
		}

		runner := consumer.New(cfg.Brokers, p, logger)
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			logger.Info("starting pipeline", "pipeline", name)
			if err := runner.Run(ctx); err != nil {
				logger.Error("pipeline stopped with error", "pipeline", name, "error", err)
			}
			if err := runner.Close(); err != nil {
				logger.Error("closing pipeline resources failed", "pipeline", name, "error", err)
			}
		}(p.Name)
	}

	wg.Wait()
	logger.Info("bridge shut down")
}
