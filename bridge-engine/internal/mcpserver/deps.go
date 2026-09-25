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
)

// ConfigSource is where the MCP config tools read the current pipelines
// from and apply changes to: the config file on a single node, or the
// cluster config topic in cluster mode.
type ConfigSource interface {
	Pipelines() []config.Pipeline
	Apply(ctx context.Context, pipelines []config.Pipeline) error
	// Mode is "file" or "cluster", so answers can say where a change goes.
	Mode() string
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
	Location *time.Location
	// AllPipelines lifts the mcp_access filter, for the console's REST
	// API where access is governed by the API tokens instead.
	AllPipelines bool
}

func (d Deps) loc() *time.Location {
	if d.Location != nil {
		return d.Location
	}
	return time.UTC
}

// localize returns t in the display timezone. JSON renders it as ISO 8601
// with that zone's offset, e.g. 2026-09-24T21:05:00+07:00.
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

// visiblePipelines is every configured pipeline an MCP client may see:
// everything except mcp_access: none.
func (d Deps) visiblePipelines() []config.Pipeline {
	if d.Config == nil {
		return nil
	}
	if d.AllPipelines {
		return d.Config.Pipelines()
	}
	var out []config.Pipeline
	for _, p := range d.Config.Pipelines() {
		if p.MCPAccess != config.MCPAccessNone {
			out = append(out, p)
		}
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
