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
	"github.com/raven-clown/ark/bridge-engine/internal/mcpserver"
)

type fileSource struct {
	path   string
	reload *configReloader

	mu       sync.Mutex
	current  []config.Pipeline
	projects []config.Project
	model    *config.AssistantModel
	settings mcpserver.Settings
	users    []config.User
}

func (f *fileSource) Mode() string { return "file" }

func (f *fileSource) set(cfg *config.Config) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current, f.projects, f.model, f.users = cfg.Pipelines, cfg.Projects, cfg.Assistant.Model, cfg.Auth.Users
	f.settings = mcpserver.Settings{Timezone: cfg.Timezone, Model: cfg.Assistant.Model, Tuning: cfg.Tuning}
}

func (f *fileSource) Settings() mcpserver.Settings {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.settings
}

func (f *fileSource) ApplySettings(_ context.Context, s mcpserver.Settings) error {
	return f.write(func(cfg *config.Config) {
		cfg.Timezone, cfg.Assistant.Model, cfg.Tuning = s.Timezone, s.Model, s.Tuning
	})
}

func (f *fileSource) Users() []config.User {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]config.User(nil), f.users...)
}

func (f *fileSource) ApplyUsers(_ context.Context, users []config.User) error {
	return f.write(func(cfg *config.Config) { cfg.Auth.Users = users })
}

func (f *fileSource) Projects() []config.Project {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]config.Project(nil), f.projects...)
}

func (f *fileSource) DefaultModel() *config.AssistantModel {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.model
}

func (f *fileSource) ApplyProjects(ctx context.Context, projects []config.Project) error {
	return f.write(func(cfg *config.Config) { cfg.Projects = projects })
}

func (f *fileSource) Pipelines() []config.Pipeline {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]config.Pipeline, len(f.current))
	copy(out, f.current)
	return out
}

func (f *fileSource) Apply(_ context.Context, pipelines []config.Pipeline) error {
	return f.write(func(cfg *config.Config) { cfg.Pipelines = pipelines })
}

func (f *fileSource) write(change func(*config.Config)) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cfg, err := config.LoadFile(f.path)
	if err != nil {
		return err
	}
	change(cfg)
	check, err := config.LoadFile(f.path)
	if err != nil {
		return err
	}
	change(check)
	if err := config.Prepare(check); err != nil {
		return err
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	header := fmt.Sprintf("# Written by ARK at %s after a confirmed change made through MCP or the console.\n# The previous version is in %s.bak.\n", time.Now().UTC().Format(time.RFC3339), filepath.Base(f.path))

	perm := os.FileMode(0o600)
	if info, err := os.Stat(f.path); err == nil {
		perm = info.Mode().Perm()
		old, err := os.ReadFile(f.path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(f.path+".bak", old, perm); err != nil { // #nosec G703 -- path is the operator-supplied -config flag, not network input
			return fmt.Errorf("the config file's directory isn't writable (mounted read-only?); change the file by hand or use cluster mode: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, append([]byte(header), body...), perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	f.mu.Unlock()
	defer f.mu.Lock()
	return f.reload.Reload()
}

type clusterSource struct {
	node     *cluster.Node
	model    *config.AssistantModel
	settings mcpserver.Settings
}

func (c clusterSource) Mode() string                 { return "cluster" }
func (c clusterSource) Pipelines() []config.Pipeline { return c.node.Pipelines() }
func (c clusterSource) Apply(ctx context.Context, pipelines []config.Pipeline) error {
	if err := config.ValidateProjects(c.node.Projects(), pipelines); err != nil {
		return err
	}
	return c.node.PublishConfig(ctx, pipelines)
}
func (c clusterSource) Projects() []config.Project { return c.node.Projects() }
func (c clusterSource) ApplyProjects(ctx context.Context, projects []config.Project) error {
	return c.node.PublishProjects(ctx, projects)
}
func (c clusterSource) DefaultModel() *config.AssistantModel { return c.model }
func (c clusterSource) Settings() mcpserver.Settings         { return c.settings }
func (c clusterSource) ApplySettings(context.Context, mcpserver.Settings) error {
	return fmt.Errorf("in cluster mode each node reads its engine settings from its own config file; change them there and reload")
}
