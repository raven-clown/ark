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
	Workers     int    `json:"workers_on_this_node"`
}

func buildServer(scope Scope, d Deps, cf *confirmations) *mcp.Server {
	reg, audit := d.Registry, d.Audit
	s := mcp.NewServer(&mcp.Implementation{Name: "ark", Title: "ARK Kafka callback bridge", Version: d.Version}, &mcp.ServerOptions{Instructions: instructions})
	registerPrompts(s)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "interpret_request",
		Description: "Call this FIRST with the user's message, word for word, in any language. It works out what they want (possibly several things), ties every pipeline/topic/URL/rate/time they mention to what actually exists in ARK, lists what's still ambiguous and should be asked back, and returns a plan of which tools to call next.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in interpretIn) (*mcp.CallToolResult, interpretOut, error) {
		return nil, interpret(d, in.Message), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_help",
		Description: "What this assistant can and can't do with the current token, where config changes go, and example questions. Call this for greetings or 'what can you do'.",
	}, func(context.Context, *mcp.CallToolRequest, emptyIn) (*mcp.CallToolResult, helpOut, error) {
		return nil, help(d, scope), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_overview",
		Description: "Health of every visible pipeline in one call (healthy / degraded / paused / down, with a one-line reason), what needs attention, cluster status, and notable events from the last hour. Start here for 'how is everything?'.",
	}, func(context.Context, *mcp.CallToolRequest, emptyIn) (*mcp.CallToolResult, overviewOut, error) {
		return nil, overview(d), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "diagnose_pipeline",
		Description: "Explain what is happening with one pipeline and why: findings ordered by severity, each with the evidence and suggested actions, plus live numbers (lag, throughput counters, callback latency, breaker, oldest stuck message), recent events and the latest dead-letter entries with their reasons.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in pipelineNameIn) (*mcp.CallToolResult, Diagnosis, error) {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return nil, Diagnosis{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		return nil, diagnose(d, p), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_recent_events",
		Description: "What happened and why, newest first: pipeline starts/stops/resizes, pauses, circuit breaker trips, messages retried in place, rejected or dead-lettered (with the reason), rate limiting, worker restarts, config changes, leader changes, DLQ redrives. Filter by pipeline, time window and kind.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in eventsIn) (*mcp.CallToolResult, eventsOut, error) {
		out, err := recentEvents(d, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "explain_error",
		Description: "Explain an error message from ARK, Kafka or a target app: which component it comes from, what it means, likely causes, how to fix it, whether it usually fixes itself, and where it recently occurred.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in explainIn) (*mcp.CallToolResult, explainErrorOut, error) {
		return nil, explainError(d, in.Error), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "recommend_tuning",
		Description: "Capacity and settings advice for one pipeline from measured callback latency and partition count: the current throughput ceiling, settings to reach target_msgs_per_sec, a best-practice review of its config, and rough node sizing.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in tuningIn) (*mcp.CallToolResult, tuningOut, error) {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return nil, tuningOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		return nil, recommendTuning(ctx, d, p, in.TargetMsgsPerSec), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "check_data",
		Description: "Look at real recent messages of a pipeline (from: source, dlq or reject) and report what's odd: invalid JSON, missing keys, fields with mixed types or missing sometimes, values that break the usual format (uuid, email, date-time, url), numeric outliers, and data rule violations. Also returns suggested data_rules drafted from what the data actually looks like.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in checkDataIn) (*mcp.CallToolResult, checkDataOut, error) {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return nil, checkDataOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		out, err := checkData(ctx, d, p, in.Sample, in.From)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "test_message",
		Description: "Predict what a pipeline would do with one message, without sending anything: which data rules it breaks, whether a fast_path_rule catches it, or whether it would go to the target.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in testMessageIn) (*mcp.CallToolResult, testMessageOut, error) {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return nil, testMessageOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		out, err := testMessage(p, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_pipeline_schema",
		Description: "Every pipeline config field: type, whether it's required, default, allowed values and what it does, plus a complete example. Use before writing or changing pipeline YAML.",
	}, func(context.Context, *mcp.CallToolRequest, emptyIn) (*mcp.CallToolResult, schemaOut, error) {
		return nil, schemaOut{Fields: pipelineSchema, Example: exampleYAML}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_pipeline_config",
		Description: "The current config of one pipeline as YAML, with every default filled in. Start from this when changing a pipeline.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, in pipelineNameIn) (*mcp.CallToolResult, pipelineConfigOut, error) {
		p, ok := d.pipeline(in.Name)
		if !ok {
			return nil, pipelineConfigOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Name)
		}
		return nil, pipelineConfigOut{YAML: toYAML(p), MCPAccess: string(p.MCPAccess)}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_topics",
		Description: "Kafka topics (internal ones excluded) with their partition counts and which pipelines use each one and how.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyIn) (*mcp.CallToolResult, topicsOut, error) {
		topics, err := listTopics(ctx, d)
		return nil, topicsOut{Topics: topics}, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "validate_pipeline_config",
		Description: "Check one pipeline's YAML without applying it: rejects unknown or misspelled fields, validates it together with the rest of the config, checks topics and partitions, and returns errors, best-practice warnings, a field-by-field diff against the current version, and the normalized YAML.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in yamlIn) (*mcp.CallToolResult, validationOut, error) {
		_, _, out := validate(ctx, d, in.YAML)
		return nil, out, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_pipelines",
		Description: "List every pipeline this token can see. Pipelines with mcp_access: none are excluded entirely, even from this list. Each entry shows its own mcp_access value, so you know before calling any other tool whether you can only read it or also write to it.",
	}, listPipelines(d))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_pipeline_status",
		Description: "Raw live status for one pipeline, per worker: counters, circuit breaker, pause state, lag, callback latency. diagnose_pipeline is usually more useful.",
	}, getPipelineStatus(reg))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_dlq_messages",
		Description: "List recent messages in a pipeline's dead_letter_topic or reject_topic (kind: \"dlq\" or \"reject\"), each with the reason ARK recorded when it routed it there, its correlation ID and how many times it was redriven.",
	}, listDLQMessages(d))

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_dlq_message",
		Description: "Get one dlq/reject entry by id (partition:offset from list_dlq_messages), including its payload and reason.",
	}, getDLQMessage(d))

	if scope == ScopeOperator || scope == ScopeAdmin {
		mcp.AddTool(s, &mcp.Tool{
			Name:        "pause_pipeline",
			Description: "Pause a pipeline (everywhere, in cluster mode). Messages wait safely in source_topic and continue exactly where they stopped once resumed. Requires mcp_access: read_write.",
		}, pausePipeline(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "resume_pipeline",
			Description: "Resume a paused pipeline. Requires mcp_access: read_write.",
		}, resumePipeline(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "retry_dlq_message",
			Description: "Resend a dlq/reject entry to the pipeline's source_topic so it's processed again from the top. It's recorded as handled, so it can't be retried twice. Requires mcp_access: read_write.",
		}, retryDLQMessage(reg, scope, audit))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "discard_dlq_message",
			Description: "Mark a dlq/reject entry as handled without retrying it, on every node and across restarts. The Kafka message itself stays until retention removes it. Requires mcp_access: read_write.",
		}, discardDLQMessage(reg, scope, audit))
	}

	if scope == ScopeAdmin && d.Config != nil {
		mcp.AddTool(s, &mcp.Tool{
			Name:        "create_pipeline",
			Description: "Create a new pipeline. First call with yaml: returns a preview (validation, warnings, diff) and a confirm_token, and changes nothing. Show the preview to the user; only after they explicitly confirm, call again with just confirm_token to apply. Fails if the name exists.",
		}, applyTool(d, cf, "create_pipeline", scope))

		mcp.AddTool(s, &mcp.Tool{
			Name:        "apply_pipeline_config",
			Description: "Create or change one pipeline (set enabled: false to stop it without deleting it). Same two steps as create_pipeline: preview with yaml, then apply with confirm_token only after the user confirms. Existing pipelines can only be changed if their mcp_access is read_write.",
		}, applyTool(d, cf, "apply_pipeline_config", scope))
	}

	return s
}

type checkDataIn struct {
	Name   string `json:"name" jsonschema:"the pipeline name"`
	Sample int    `json:"sample,omitempty" jsonschema:"how many recent messages to look at, default 100"`
	From   string `json:"from,omitempty" jsonschema:"source (default), dlq or reject"`
}

type interpretIn struct {
	Message string `json:"message" jsonschema:"the user's message exactly as they wrote it"`
}

type explainIn struct {
	Error string `json:"error" jsonschema:"the error message or log line to explain"`
}

type tuningIn struct {
	Name             string  `json:"name" jsonschema:"the pipeline name"`
	TargetMsgsPerSec float64 `json:"target_msgs_per_sec,omitempty" jsonschema:"desired throughput, if the user has a goal"`
}

type schemaOut struct {
	Fields  []fieldDoc `json:"fields"`
	Example string     `json:"example_yaml"`
}

type pipelineConfigOut struct {
	YAML      string `json:"yaml"`
	MCPAccess string `json:"mcp_access"`
}

type topicsOut struct {
	Topics []topicInfo `json:"topics"`
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

func listPipelines(d Deps) mcp.ToolHandlerFor[emptyIn, listPipelinesOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, _ emptyIn) (*mcp.CallToolResult, listPipelinesOut, error) {
		var out listPipelinesOut
		for _, p := range d.visiblePipelines() {
			out.Pipelines = append(out.Pipelines, PipelineSummary{
				Name:        p.Name,
				Tenant:      p.Tenant,
				MCPAccess:   string(p.MCPAccess),
				SourceTopic: p.SourceTopic,
				Destination: p.DestinationTopic,
				Enabled:     p.IsEnabled(),
				Workers:     len(d.Registry.PipelineRunners(p.Name)),
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
		b, access, ok := reg.DLQBrowser(name, kind)
		if !ok {
			return nil, "", fmt.Errorf("pipeline not found: %s", name)
		}
		if !visibleToMCP(string(access)) {
			return nil, "", fmt.Errorf("pipeline %s has mcp_access: none", name)
		}
		return b, string(access), nil
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

func listDLQMessages(d Deps) mcp.ToolHandlerFor[listDLQIn, listDLQOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in listDLQIn) (*mcp.CallToolResult, listDLQOut, error) {
		browser, _, err := findDLQBrowser(d.Registry, in.Name, in.Kind)
		if err != nil {
			return nil, listDLQOut{}, err
		}
		return nil, listDLQOut{Entries: d.localizeEntries(browser.List())}, nil
	}
}

type dlqEntryIn struct {
	Name string `json:"name" jsonschema:"the pipeline name"`
	Kind string `json:"kind" jsonschema:"\"dlq\" for dead_letter_topic or \"reject\" for reject_topic"`
	ID   string `json:"id" jsonschema:"the entry id from list_dlq_messages, formatted partition:offset"`
}

func getDLQMessage(d Deps) mcp.ToolHandlerFor[dlqEntryIn, dlq.Entry] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in dlqEntryIn) (*mcp.CallToolResult, dlq.Entry, error) {
		browser, _, err := findDLQBrowser(d.Registry, in.Name, in.Kind)
		if err != nil {
			return nil, dlq.Entry{}, err
		}
		entry, ok := browser.Get(in.ID)
		if !ok {
			return nil, dlq.Entry{}, fmt.Errorf("entry not found: %s", in.ID)
		}
		return nil, d.localizeEntries([]dlq.Entry{entry})[0], nil
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
	return func(ctx context.Context, _ *mcp.CallToolRequest, in dlqEntryIn) (*mcp.CallToolResult, actionOut, error) {
		browser, access, err := findDLQBrowser(reg, in.Name, in.Kind)
		if err != nil {
			return nil, actionOut{}, err
		}
		if err := requireWritable(access, in.Name); err != nil {
			return nil, actionOut{}, err
		}
		found, err := browser.Discard(ctx, in.ID)
		if err != nil {
			return nil, actionOut{}, err
		}
		if !found {
			return nil, actionOut{}, fmt.Errorf("entry not found: %s", in.ID)
		}
		audit.Info("mcp write", "scope", scope, "action", "discard_dlq_message", "pipeline", in.Name, "kind", in.Kind, "id", in.ID)
		return nil, actionOut{State: "discarded"}, nil
	}
}
