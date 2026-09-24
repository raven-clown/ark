package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// fileSource applies MCP config changes on a single node by rewriting the
// config file and reloading it, the same path an operator's edit takes.
type fileSource struct {
	path   string
	reload *configReloader

	mu      sync.Mutex
	current []config.Pipeline
}

func (f *fileSource) Mode() string { return "file" }

func (f *fileSource) set(pipelines []config.Pipeline) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current = pipelines
}

func (f *fileSource) Pipelines() []config.Pipeline {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]config.Pipeline, len(f.current))
	copy(out, f.current)
	return out
}

// Apply writes pipelines into the config file (keeping brokers, topics and
// cluster settings), keeps the previous file as <path>.bak, and reloads.
// Comments in the file are not preserved.
func (f *fileSource) Apply(_ context.Context, pipelines []config.Pipeline) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cfg, err := config.Load(f.path)
	if err != nil {
		return err
	}
	cfg.Pipelines = pipelines
	if err := cfg.Validate(); err != nil {
		return err
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	header := fmt.Sprintf("# Written by ARK at %s after a confirmed change made through MCP.\n# The previous version is in %s.bak.\n", time.Now().UTC().Format(time.RFC3339), filepath.Base(f.path))

	info, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	old, err := os.ReadFile(f.path)
	if err != nil {
		return err
	}
	if err := os.WriteFile(f.path+".bak", old, info.Mode().Perm()); err != nil { // #nosec G703 -- path is the operator-supplied -config flag, not network input
		return fmt.Errorf("the config file's directory isn't writable (mounted read-only?); change the file by hand or use cluster mode: %w", err)
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, append([]byte(header), body...), info.Mode().Perm()); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	f.mu.Unlock()
	defer f.mu.Lock()
	return f.reload.Reload()
}

// clusterSource applies MCP config changes by publishing them to the
// cluster config topic, which every node applies.
type clusterSource struct{ node *cluster.Node }

func (c clusterSource) Mode() string                 { return "cluster" }
func (c clusterSource) Pipelines() []config.Pipeline { return c.node.Pipelines() }
func (c clusterSource) Apply(ctx context.Context, pipelines []config.Pipeline) error {
	return c.node.PublishConfig(ctx, pipelines)
}
