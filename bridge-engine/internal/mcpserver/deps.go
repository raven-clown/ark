package mcpserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

type ConfigSource interface {
	Pipelines() []config.Pipeline
	Apply(ctx context.Context, pipelines []config.Pipeline) error
	// Mode is "file" or "cluster", so answers can say where a change goes.
	Mode() string
	Projects() []config.Project
	ApplyProjects(ctx context.Context, projects []config.Project) error
	// DefaultModel is assistant.model, for pipelines outside a project.
	DefaultModel() *config.AssistantModel
	Settings() Settings
	ApplySettings(ctx context.Context, s Settings) error
}

// Settings are the engine-wide values the console can edit.
type Settings struct {
	Timezone string                 `json:"timezone"`
	Model    *config.AssistantModel `json:"assistant_model,omitempty"`
	Tuning   tuning.Values          `json:"tuning"`
}

type Deps struct {
	Registry          api.Registry
	Config            ConfigSource
	Brokers           []string
	ReplicationFactor int
	Events            *events.Log
	Cluster           *cluster.Node // nil unless cluster mode is on
	Audit             *slog.Logger
	Version           string
	// Location is the configured display timezone; nil means UTC.
	Location     *time.Location
	AllPipelines bool
	Project      string
	// History is what get_metrics reads; the console samples into it.
	History  *History
	Sources  func() []config.Source
	Accounts AccountStore
}

func (d Deps) loc() *time.Location {
	if d.Location != nil {
		return d.Location
	}
	return time.UTC
}

func (d Deps) localize(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.In(d.loc()).Truncate(time.Millisecond)
}

func (d Deps) localizeISO(s string) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return s
	}
	return d.localize(t).Format(time.RFC3339)
}

func (d Deps) localizeEvents(evs []events.Event) []events.Event {
	out := make([]events.Event, len(evs))
	for i, e := range evs {
		e.Time = d.localize(e.Time)
		out[i] = e
	}
	return out
}

func (d Deps) localizeEntries(entries []dlq.Entry) []dlq.Entry {
	out := make([]dlq.Entry, len(entries))
	for i, e := range entries {
		e.Timestamp = d.localize(e.Timestamp)
		e.FailedAt = d.localizeISO(e.FailedAt)
		out[i] = e
	}
	return out
}

func (d Deps) events() *events.Log {
	if d.Events != nil {
		return d.Events
	}
	return events.Default
}

func (d Deps) projectAccess(project string) config.AIAccess {
	if project == "" || d.Config == nil {
		return config.AIAccessConfigure
	}
	for _, p := range d.Config.Projects() {
		if p.Name == project {
			return p.AIAccess
		}
	}
	return config.AIAccessNone
}

func (d Deps) visiblePipelines() []config.Pipeline {
	if d.Config == nil {
		return nil
	}
	if d.AllPipelines {
		return d.Config.Pipelines()
	}
	var out []config.Pipeline
	for _, p := range d.Config.Pipelines() {
		if d.Project != "" && p.Project != d.Project {
			continue
		}
		acc := d.projectAccess(p.Project)
		if p.MCPAccess == config.MCPAccessNone || acc == config.AIAccessNone {
			continue
		}
		if acc == config.AIAccessReadOnly && p.MCPAccess == config.MCPAccessReadWrite {
			p.MCPAccess = config.MCPAccessReadOnly
		}
		out = append(out, p)
	}
	return out
}

func (d Deps) pipeline(name string) (config.Pipeline, bool) {
	for _, p := range d.visiblePipelines() {
		if p.Name == name {
			return p, true
		}
	}
	return config.Pipeline{}, false
}
