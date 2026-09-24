package mcpserver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/events"
)

// instructions are sent to the client on connect and tell the model how to
// be a useful ARK assistant with these tools, whatever the user asks.
const instructions = `You are connected to ARK, a Kafka callback bridge: each pipeline consumes a Kafka topic, calls an HTTP endpoint per message, and produces the response to another topic, with retries, a circuit breaker, dead-letter and reject topics, and rules that can route messages without calling the endpoint.

How to help:
- Understand first: for every user message, call interpret_request with their exact words. Read its restated_request, the pipelines it matched (with confidence) and its plan. If ask_the_user lists something that no tool can answer, ask the user that (briefly, one question at a time) before acting. Then follow the plan, and if results raise a new question, investigate further before answering.
- Greetings, "what can you do", or an open question: call get_help (what this token can do) and get_overview (current state), then answer briefly and suggest a few useful next questions.
- "How is X / what's happening / why is X slow, stuck, failing": call diagnose_pipeline. Explain in plain words what is happening, why (quote the evidence it returns), and what to do. Offer follow-ups (show DLQ entries, recent events, tuning).
- An error message or log line: call explain_error with the text. Say where it comes from, what it means, and how to fix it.
- "What happened (recently / at 3am / to X)": call get_recent_events.
- Odd data, bad formats, strange fields or parameters: call check_data (and test_message for a specific example). To catch such data from now on, propose data_rules (check_data returns a draft), start with on_violation: tag, and apply through the confirm flow.
- Capacity, performance, sizing, "how should I configure": call recommend_tuning (pass target_msgs_per_sec if the user has a goal).
- Creating or changing a pipeline: call get_pipeline_schema and list_topics, draft YAML, call validate_pipeline_config and fix every error, then call create_pipeline or apply_pipeline_config WITHOUT a confirm_token to get a preview. Show the user the diff and warnings and ask for explicit confirmation. Only after they agree, call it again with the confirm_token. Never confirm on the user's behalf.
- Never invent numbers or states; everything you report must come from a tool result. Say when something isn't known.
- Answer in the language the user wrote in (Thai, English, simplified or traditional Chinese, or any other), unless they ask otherwise. Keep answers short and concrete; use lists for findings and actions.`

type helpOut struct {
	YourScope   string   `json:"your_scope"`
	ConfigMode  string   `json:"config_changes_go_to"`
	CanDo       []string `json:"can_do"`
	CannotDo    []string `json:"cannot_do,omitempty"`
	TryAsking   []string `json:"try_asking"`
	Timezone    string   `json:"timezone"`
	ARKVersion  string   `json:"ark_version"`
	ClusterMode bool     `json:"cluster_mode"`
}

func help(d Deps, scope Scope) helpOut {
	out := helpOut{YourScope: string(scope), ARKVersion: d.Version, ClusterMode: d.Cluster != nil, Timezone: d.loc().String()}
	if d.Config != nil {
		out.ConfigMode = map[string]string{"file": "the config file on this ARK node (hot-reloaded)", "cluster": "the cluster config topic (every node applies it)"}[d.Config.Mode()]
	}
	out.CanDo = []string{
		"Give an overview of every pipeline's health",
		"Diagnose a pipeline: what is happening, why, and what to do",
		"Explain an error message: where it comes from and how to fix it",
		"Show recent events (starts, pauses, breaker trips, rejects, dead letters) with reasons",
		"Recommend settings and sizing for a throughput goal, and review a pipeline's config",
		"Check real messages for odd formats, missing or mixed-type fields, outliers and bad keys, and draft data rules to catch them",
		"Predict what a pipeline would do with a given message (test_message)",
		"Show a pipeline's config, the config schema, and Kafka topics",
		"Validate a pipeline config without applying it",
		"List and inspect dead-letter and reject entries with their reasons",
	}
	if scope == ScopeOperator || scope == ScopeAdmin {
		out.CanDo = append(out.CanDo, "Pause and resume pipelines, retry or discard dead-letter entries (pipelines with mcp_access: read_write)")
	} else {
		out.CannotDo = append(out.CannotDo, "Pause/resume or retry/discard (needs an operator token)")
	}
	if scope == ScopeAdmin {
		out.CanDo = append(out.CanDo, "Create pipelines and change their config, after you confirm a preview")
	} else {
		out.CannotDo = append(out.CannotDo, "Create or change pipelines (needs an admin token)")
	}
	out.TryAsking = []string{
		"How is everything running?",
		"Why is orders-processor slow?",
		"What does 'Group Coordinator Not Available' mean?",
		"What happened to payments in the last hour?",
		"How should I configure orders to handle 2000 messages per second?",
		"Create a pipeline that sends orders.raw to http://fraud:8080/check",
		"Is there any weird data coming into orders?",
	}
	return out
}

type overviewPipeline struct {
	Name    string `json:"name"`
	Health  string `json:"health"`
	Summary string `json:"summary"`
	Lag     int64  `json:"lag"`
	DLQ     int    `json:"pending_dlq"`
}

type overviewOut struct {
	Time            string             `json:"time"`
	Timezone        string             `json:"timezone"`
	Pipelines       []overviewPipeline `json:"pipelines"`
	NeedsAttention  []string           `json:"needs_attention"`
	Cluster         any                `json:"cluster,omitempty"`
	RecentHighlight []events.Event     `json:"recent_highlights,omitempty"`
}

func overview(d Deps) overviewOut {
	out := overviewOut{Time: d.localize(time.Now()).Format(time.RFC3339), Timezone: d.loc().String()}
	for _, p := range d.visiblePipelines() {
		diag := diagnose(d, p)
		out.Pipelines = append(out.Pipelines, overviewPipeline{Name: p.Name, Health: diag.Health, Summary: diag.Summary, Lag: diag.Numbers.Lag, DLQ: diag.Numbers.PendingDLQ})
		if diag.Health != "healthy" {
			out.NeedsAttention = append(out.NeedsAttention, fmt.Sprintf("%s (%s): %s", p.Name, diag.Health, diag.Summary))
		}
	}
	rank := map[string]int{"down": 0, "degraded": 1, "paused": 2, "healthy": 3}
	sort.SliceStable(out.Pipelines, func(i, j int) bool { return rank[out.Pipelines[i].Health] < rank[out.Pipelines[j].Health] })
	if d.Cluster != nil {
		out.Cluster = d.Cluster.StatusSnapshot()
	}
	out.RecentHighlight = d.localizeEvents(d.events().Recent("", time.Now().Add(-time.Hour), 10,
		events.BreakerOpened, events.BreakerClosed, events.PipelineStartFailed, events.Paused, events.Resumed, events.ConfigApplied, events.LeaderChanged, events.WorkerRestarted))
	return out
}

type eventsIn struct {
	Pipeline     string   `json:"pipeline,omitempty" jsonschema:"only events for this pipeline; empty for all"`
	SinceMinutes int      `json:"since_minutes,omitempty" jsonschema:"only events from the last N minutes; 0 for any time"`
	Kinds        []string `json:"kinds,omitempty" jsonschema:"only these event kinds, e.g. breaker_opened, message_dead_lettered, message_rejected, paused"`
	Limit        int      `json:"limit,omitempty" jsonschema:"max events, default 50"`
}

type eventsOut struct {
	Events []events.Event `json:"events"`
	Note   string         `json:"note"`
}

func recentEvents(d Deps, in eventsIn) (eventsOut, error) {
	if in.Pipeline != "" {
		if _, ok := d.pipeline(in.Pipeline); !ok {
			return eventsOut{}, fmt.Errorf("pipeline not found or not visible: %s", in.Pipeline)
		}
	}
	limit := in.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var since time.Time
	if in.SinceMinutes > 0 {
		since = time.Now().Add(-time.Duration(in.SinceMinutes) * time.Minute)
	}
	kinds := make([]events.Kind, len(in.Kinds))
	for i, k := range in.Kinds {
		kinds[i] = events.Kind(k)
	}
	all := d.events().Recent(in.Pipeline, since, limit*2, kinds...)
	visible := map[string]bool{"": true}
	for _, p := range d.visiblePipelines() {
		visible[p.Name] = true
	}
	var out eventsOut
	for _, e := range all {
		if visible[e.Pipeline] && len(out.Events) < limit {
			e.Time = d.localize(e.Time)
			out.Events = append(out.Events, e)
		}
	}
	out.Note = "Times are ISO 8601 in " + d.loc().String() + ". Newest first. This node keeps the last 2000 events in memory since it started; in a cluster each node records what happened on it."
	return out, nil
}

func registerPrompts(s *mcp.Server) {
	text := func(t string) []*mcp.PromptMessage {
		return []*mcp.PromptMessage{{Role: "user", Content: &mcp.TextContent{Text: t}}}
	}
	s.AddPrompt(&mcp.Prompt{Name: "ark_overview", Title: "How is everything running?", Description: "Overview of every pipeline, what needs attention and why."},
		func(context.Context, *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{Messages: text("Give me an overview of ARK right now: which pipelines are healthy, which need attention, why, and what I should do. Use get_overview, then diagnose_pipeline for anything not healthy.")}, nil
		})
	s.AddPrompt(&mcp.Prompt{Name: "ark_diagnose", Title: "Why is this pipeline misbehaving?", Description: "Find out what's wrong with one pipeline and how to fix it.",
		Arguments: []*mcp.PromptArgument{{Name: "pipeline", Description: "pipeline name", Required: true}}},
		func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{Messages: text(fmt.Sprintf("Diagnose the pipeline %q: what is happening, why, and what should I do? Use diagnose_pipeline, then look at recent events and DLQ entries if they help explain it.", req.Params.Arguments["pipeline"]))}, nil
		})
	s.AddPrompt(&mcp.Prompt{Name: "ark_explain_error", Title: "What does this error mean?", Description: "Explain an error message and how to fix it.",
		Arguments: []*mcp.PromptArgument{{Name: "error", Description: "the error text", Required: true}}},
		func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{Messages: text(fmt.Sprintf("Explain this ARK/Kafka error, where it comes from, and how to fix it: %s", req.Params.Arguments["error"]))}, nil
		})
	s.AddPrompt(&mcp.Prompt{Name: "ark_create_pipeline", Title: "Create a pipeline from a description", Description: "Draft, validate and (after you confirm) apply a new pipeline.",
		Arguments: []*mcp.PromptArgument{{Name: "description", Description: "what the pipeline should do, in plain words", Required: true}}},
		func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{Messages: text("Create an ARK pipeline for this: " + req.Params.Arguments["description"] + ". Use get_pipeline_schema and list_topics, ask me about anything you'd otherwise have to guess (target URL, topics), validate it, show me the preview, and only apply after I confirm.")}, nil
		})
	s.AddPrompt(&mcp.Prompt{Name: "ark_tune", Title: "Tune a pipeline for a throughput goal", Description: "Sizing and settings for a target rate.",
		Arguments: []*mcp.PromptArgument{{Name: "pipeline", Required: true}, {Name: "target_msgs_per_sec", Description: "goal, e.g. 2000"}}},
		func(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			return &mcp.GetPromptResult{Messages: text(fmt.Sprintf("How should I configure and size %q to handle %s messages per second? Use recommend_tuning and explain the numbers.", req.Params.Arguments["pipeline"], req.Params.Arguments["target_msgs_per_sec"]))}, nil
		})
}
