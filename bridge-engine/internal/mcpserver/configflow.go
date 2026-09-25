package mcpserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

const actionDelete = "delete_pipeline"

func applyTool(d Deps, cf *confirmations, action string, scope Scope) mcp.ToolHandlerFor[applyIn, applyOut] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in applyIn) (*mcp.CallToolResult, applyOut, error) {
		user := callerID(req)
		if in.ConfirmToken != "" {
			out, err := confirmChange(ctx, d, cf, user, in.ConfirmToken, func(a string) bool { return a == action }, "MCP", string(scope))
			if err == nil {
				out.NextStep = "Call diagnose_pipeline in a few seconds to confirm it's running as expected."
			}
			return nil, out, err
		}
		if in.YAML == "" {
			return nil, applyOut{}, fmt.Errorf("yaml is required for the preview call")
		}
		out, err := previewChange(ctx, d, cf, action, user, in.YAML)
		if err == nil && out.State == "awaiting_confirmation" {
			out.NextStep = "Show the user the diff and warnings in plain words and ask them to confirm. Only after they explicitly agree, call this tool again with just confirm_token. The token expires in 10 minutes."
		}
		return nil, out, err
	}
}

// previewChange validates a create or update and, if it would change
// something, returns a confirm token that applies exactly this change.
// "create_pipeline" refuses to touch an existing pipeline. MCP callers are
// also held to each pipeline's mcp_access.
func previewChange(ctx context.Context, d Deps, cf *confirmations, action, user, src string) (applyOut, error) {
	p, existing, preview := validate(ctx, d, src)
	if action == "create_pipeline" && existing != nil {
		return applyOut{}, fmt.Errorf("pipeline %s already exists; use apply_pipeline_config to change it", p.Name)
	}
	if !d.AllPipelines {
		for _, proj := range []string{p.Project, projectOf(existing)} {
			if acc := d.projectAccess(proj); acc != config.AIAccessConfigure {
				return applyOut{}, fmt.Errorf("project %s allows AI access up to %s; changing its pipelines needs configure", proj, acc)
			}
		}
		if existing != nil && existing.MCPAccess != config.MCPAccessReadWrite {
			return applyOut{}, fmt.Errorf("pipeline %s has mcp_access: %s; only an operator editing the config directly can change it", p.Name, existing.MCPAccess)
		}
		if existing == nil {
			if _, hidden := hiddenPipeline(d, p.Name); hidden {
				return applyOut{}, fmt.Errorf("a pipeline named %s exists but isn't visible to MCP", p.Name)
			}
		}
	}
	if !preview.Valid {
		return applyOut{State: "invalid", Preview: &preview, NextStep: "Fix the errors and preview again."}, nil
	}
	if preview.Change == "unchanged" {
		return applyOut{State: "unchanged", Preview: &preview}, nil
	}
	token := cf.put(pendingChange{action: action, pipeline: p, baseHash: pipelineHash(existing), userID: user, expires: time.Now().Add(tuning.ConfirmToken())})
	return applyOut{State: "awaiting_confirmation", Preview: &preview, ConfirmToken: token}, nil
}

// previewDelete returns a confirm token that removes pipeline name.
func previewDelete(d Deps, cf *confirmations, user, name string) (applyOut, error) {
	var existing *config.Pipeline
	for _, p := range d.Config.Pipelines() {
		if p.Name == name {
			c := p
			existing = &c
		}
	}
	if existing == nil {
		return applyOut{}, fmt.Errorf("pipeline not found: %s", name)
	}
	preview := validationOut{Valid: true, Change: "delete", AppliesTo: d.Config.Mode(), Diff: []string{"pipeline " + name + ": removed"},
		Warnings: []Finding{{Severity: SeverityWarning, What: "Deleting stops the pipeline on every node. Its topics and consumer group offsets stay in Kafka, so creating it again later resumes where it stopped."}}}
	token := cf.put(pendingChange{action: actionDelete, pipeline: *existing, baseHash: pipelineHash(existing), userID: user, expires: time.Now().Add(tuning.ConfirmToken())})
	return applyOut{State: "awaiting_confirmation", Preview: &preview, ConfirmToken: token}, nil
}

// confirmChange applies a previewed change once, for the caller it was
// previewed for, and only if the pipeline hasn't changed since.
func confirmChange(ctx context.Context, d Deps, cf *confirmations, user, token string, accept func(action string) bool, via, scope string) (applyOut, error) {
	pc, err := cf.take(token, user)
	if err != nil {
		return applyOut{}, err
	}
	if !accept(pc.action) {
		return applyOut{}, fmt.Errorf("that confirm_token was issued for %s", pc.action)
	}
	pipelines := d.Config.Pipelines()
	var current *config.Pipeline
	for i := range pipelines {
		if pipelines[i].Name == pc.pipeline.Name {
			c := pipelines[i]
			current = &c
		}
	}
	if pipelineHash(current) != pc.baseHash {
		return applyOut{}, fmt.Errorf("pipeline %s changed since the preview; preview it again", pc.pipeline.Name)
	}
	updated := make([]config.Pipeline, 0, len(pipelines)+1)
	replaced := false
	for _, q := range pipelines {
		if q.Name == pc.pipeline.Name {
			replaced = true
			if pc.action != actionDelete {
				updated = append(updated, pc.pipeline)
			}
			continue
		}
		updated = append(updated, q)
	}
	if !replaced {
		updated = append(updated, pc.pipeline)
	}
	if err := d.Config.Apply(ctx, updated); err != nil {
		return applyOut{}, fmt.Errorf("applying: %w", err)
	}
	verb := "created"
	switch {
	case pc.action == actionDelete:
		verb = "deleted"
	case replaced:
		verb = "updated"
	}
	if d.Audit != nil {
		d.Audit.Info(strings.ToLower(via)+" write", "scope", scope, "action", pc.action, "pipeline", pc.pipeline.Name, "applies_to", d.Config.Mode())
	}
	events.Record(pc.pipeline.Name, events.ConfigApplied, fmt.Sprintf("pipeline config %s through %s (%s scope)", verb, via, scope), nil)
	return applyOut{State: "applied"}, nil
}

func projectOf(p *config.Pipeline) string {
	if p == nil {
		return ""
	}
	return p.Project
}
