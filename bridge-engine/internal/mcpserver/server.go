package mcpserver

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
)

type PipelineSummary struct {
	Name        string `json:"name"`
	Tenant      string `json:"tenant,omitempty"`
	MCPAccess   string `json:"mcp_access"`
	SourceTopic string `json:"source_topic"`
	Destination string `json:"destination_topic,omitempty"`
	Enabled     bool   `json:"enabled"`
	Workers     int    `json:"workers"`
}

func buildServer(scope Scope, reg api.Registry, audit *slog.Logger) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "ark", Version: "0.2.0"}, nil)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_pipelines",
		Description: "List every pipeline this token can see. Pipelines with mcp_access: none are excluded entirely, even from this list. Each entry shows its own mcp_access value, so you know before calling any other tool whether you can only read it or also write to it.",
	}, listPipelines(reg))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_pipeline_status",
		Description: "Get live status for one pipeline: throughput, circuit breaker state, pause state, and per-worker detail. Fails if the pipeline's mcp_access is none.",
	}, getPipelineStatus(reg))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_dlq_messages",
		Description: "List recent messages sitting in a pipeline's dead_letter_topic or reject_topic (kind: \"dlq\" or \"reject\"). This is a bounded recent-history view, not the full topic.",
	}, listDLQMessages(reg))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_dlq_message",
		Description: "Get one specific dlq/reject entry by id (the id returned from list_dlq_messages, formatted partition:offset).",
	}, getDLQMessage(reg))

	if scope == ScopeOperator || scope == ScopeAdmin {
		mcp.AddTool(s, &mcp.Tool{
			Name:        "pause_pipeline",
			Description: "Pause a pipeline's workers without stopping the process. Messages queue up safely in source_topic and resume processing exactly where they left off once resumed. Requires the pipeline's mcp_access to be read_write.",
		}, pausePipeline(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "resume_pipeline",
			Description: "Resume a paused pipeline. Requires the pipeline's mcp_access to be read_write.",
		}, resumePipeline(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "retry_dlq_message",
			Description: "Re-produce a dlq/reject entry back into the pipeline's source_topic, so it's processed again from the top (fast_path_rules included). Removes the entry from the dlq/reject listing either way; the underlying Kafka message is never deleted. Requires the pipeline's mcp_access to be read_write.",
		}, retryDLQMessage(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "discard_dlq_message",
			Description: "Remove a dlq/reject entry from the listing without retrying it. This only affects the listing ARK keeps in memory; it does not delete the underlying Kafka message. Requires the pipeline's mcp_access to be read_write.",
		}, discardDLQMessage(reg, scope, audit))
	}

	return s
}

func pipelineStatuses(reg api.Registry, name string) []consumer.Status {
	runners := reg.PipelineRunners(name)
	statuses := make([]consumer.Status, 0, len(runners))
	for _, r := range runners {
		statuses = append(statuses, r.Status())
	}
	return statuses
}

func visibleToMCP(access string) bool {
	return config.MCPAccess(access) != config.MCPAccessNone
}

func writableByMCP(access string) bool {
	return config.MCPAccess(access) == config.MCPAccessReadWrite
}

type emptyIn struct{}

type listPipelinesOut struct {
	Pipelines []PipelineSummary `json:"pipelines"`
}

func listPipelines(reg api.Registry) mcp.ToolHandlerFor[emptyIn, listPipelinesOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, _ emptyIn) (*mcp.CallToolResult, listPipelinesOut, error) {
		seen := map[string]bool{}
		var out listPipelinesOut
		for _, r := range reg.Runners() {
			if seen[r.Name()] {
				continue
			}
			seen[r.Name()] = true
			st := r.Status()
			if !visibleToMCP(st.MCPAccess) {
				continue
			}
			out.Pipelines = append(out.Pipelines, PipelineSummary{
				Name:        st.Pipeline,
				Tenant:      st.Tenant,
				MCPAccess:   st.MCPAccess,
				SourceTopic: st.SourceTopic,
				Destination: st.Destination,
				Enabled:     st.Enabled,
				Workers:     len(reg.PipelineRunners(st.Pipeline)),
			})
		}
		return nil, out, nil
	}
}

type pipelineNameIn struct {
	Name string `json:"name" jsonschema:"the pipeline name, as returned by list_pipelines"`
}

type getPipelineStatusOut struct {
	Workers []consumer.Status `json:"workers"`
}

func getPipelineStatus(reg api.Registry) mcp.ToolHandlerFor[pipelineNameIn, getPipelineStatusOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in pipelineNameIn) (*mcp.CallToolResult, getPipelineStatusOut, error) {
		statuses := pipelineStatuses(reg, in.Name)
		if len(statuses) == 0 {
			return nil, getPipelineStatusOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		if !visibleToMCP(statuses[0].MCPAccess) {
			return nil, getPipelineStatusOut{}, fmt.Errorf("pipeline %s has mcp_access: none", in.Name)
		}
		return nil, getPipelineStatusOut{Workers: statuses}, nil
	}
}

func findDLQBrowser(reg api.Registry, name, kind string) (*dlq.Browser, string, error) {
	statuses := pipelineStatuses(reg, name)
	if len(statuses) == 0 {
		return nil, "", fmt.Errorf("pipeline not found: %s", name)
	}
	if !visibleToMCP(statuses[0].MCPAccess) {
		return nil, "", fmt.Errorf("pipeline %s has mcp_access: none", name)
	}

	var browser *dlq.Browser
	for _, r := range reg.PipelineRunners(name) {
		switch kind {
		case "dlq":
			browser = r.DLQBrowser()
		case "reject":
			browser = r.RejectBrowser()
		default:
			return nil, "", fmt.Errorf("kind must be \"dlq\" or \"reject\", got %q", kind)
		}
		if browser != nil {
			break
		}
	}
	if browser == nil {
		return nil, "", fmt.Errorf("pipeline %s has no %s topic configured", name, kind)
	}
	return browser, statuses[0].MCPAccess, nil
}

type listDLQIn struct {
	Name string `json:"name" jsonschema:"the pipeline name"`
	Kind string `json:"kind" jsonschema:"\"dlq\" for dead_letter_topic or \"reject\" for reject_topic"`
}

type listDLQOut struct {
	Entries []dlq.Entry `json:"entries"`
}

func listDLQMessages(reg api.Registry) mcp.ToolHandlerFor[listDLQIn, listDLQOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in listDLQIn) (*mcp.CallToolResult, listDLQOut, error) {
		browser, _, err := findDLQBrowser(reg, in.Name, in.Kind)
		if err != nil {
			return nil, listDLQOut{}, err
		}
		return nil, listDLQOut{Entries: browser.List()}, nil
	}
}

type dlqEntryIn struct {
	Name string `json:"name" jsonschema:"the pipeline name"`
	Kind string `json:"kind" jsonschema:"\"dlq\" for dead_letter_topic or \"reject\" for reject_topic"`
	ID   string `json:"id" jsonschema:"the entry id from list_dlq_messages, formatted partition:offset"`
}

func getDLQMessage(reg api.Registry) mcp.ToolHandlerFor[dlqEntryIn, dlq.Entry] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in dlqEntryIn) (*mcp.CallToolResult, dlq.Entry, error) {
		browser, _, err := findDLQBrowser(reg, in.Name, in.Kind)
		if err != nil {
			return nil, dlq.Entry{}, err
		}
		entry, ok := browser.Get(in.ID)
		if !ok {
			return nil, dlq.Entry{}, fmt.Errorf("entry not found: %s", in.ID)
		}
		return nil, entry, nil
	}
}

type actionOut struct {
	State string `json:"state"`
}

func requireWritable(access, pipeline string) error {
	if !writableByMCP(access) {
		return fmt.Errorf("pipeline %s has mcp_access: %s, write tools require read_write", pipeline, access)
	}
	return nil
}

func pausePipeline(reg api.Registry, scope Scope, audit *slog.Logger) mcp.ToolHandlerFor[pipelineNameIn, actionOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in pipelineNameIn) (*mcp.CallToolResult, actionOut, error) {
		statuses := pipelineStatuses(reg, in.Name)
		if len(statuses) == 0 {
			return nil, actionOut{}, fmt.Errorf("pipeline not found: %s", in.Name)
		}
		if err := requireWritable(statuses[0].MCPAccess, in.Name); err != nil {
			return nil, actionOut{}, err
		}
		reg.SetPaused(in.Name, true)
		audit.Info("mcp write", "scope", scope, "action", "pause_pipeline", "pipeline", in.Name)
		return nil, actionOut{State: "paused"}, nil
	}
}

func resumePipeline(reg api.Registry, scope Scope, audit *slog.Logger) mcp.ToolHandlerFor[pipelineNameIn, actionOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in pipelineNameIn) (*mcp.CallToolResult, actionOut, error) {
		statuses := pipelineStatuses(reg, in.Name)
		if len(statuses) == 0 {
			return nil, actionOut{}, fmt.Errorf("pipeline not found: %s", in.Name)
		}
		if err := requireWritable(statuses[0].MCPAccess, in.Name); err != nil {
			return nil, actionOut{}, err
		}
		reg.SetPaused(in.Name, false)
		audit.Info("mcp write", "scope", scope, "action", "resume_pipeline", "pipeline", in.Name)
		return nil, actionOut{State: "running"}, nil
	}
}

func retryDLQMessage(reg api.Registry, scope Scope, audit *slog.Logger) mcp.ToolHandlerFor[dlqEntryIn, actionOut] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in dlqEntryIn) (*mcp.CallToolResult, actionOut, error) {
		browser, access, err := findDLQBrowser(reg, in.Name, in.Kind)
		if err != nil {
			return nil, actionOut{}, err
		}
		if err := requireWritable(access, in.Name); err != nil {
			return nil, actionOut{}, err
		}
		if err := browser.Retry(ctx, in.ID); err != nil {
			return nil, actionOut{}, err
		}
		audit.Info("mcp write", "scope", scope, "action", "retry_dlq_message", "pipeline", in.Name, "kind", in.Kind, "id", in.ID)
		return nil, actionOut{State: "retried"}, nil
	}
}

func discardDLQMessage(reg api.Registry, scope Scope, audit *slog.Logger) mcp.ToolHandlerFor[dlqEntryIn, actionOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in dlqEntryIn) (*mcp.CallToolResult, actionOut, error) {
		browser, access, err := findDLQBrowser(reg, in.Name, in.Kind)
		if err != nil {
			return nil, actionOut{}, err
		}
		if err := requireWritable(access, in.Name); err != nil {
			return nil, actionOut{}, err
		}
		if !browser.Discard(in.ID) {
			return nil, actionOut{}, fmt.Errorf("entry not found: %s", in.ID)
		}
		audit.Info("mcp write", "scope", scope, "action", "discard_dlq_message", "pipeline", in.Name, "kind", in.Kind, "id", in.ID)
		return nil, actionOut{State: "discarded"}, nil
	}
}
