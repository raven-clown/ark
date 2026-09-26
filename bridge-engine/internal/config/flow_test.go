package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const flowPipeline = `
name: orders
source_topic: orders.raw
consumer_group: orders
dead_letter_topic: orders.dlq
reject_topic: orders.rejected
flow:
  start: [check]
  steps:
    - id: check
      type: data_check
      rules:
        fields:
          - {path: order_id, required: true}
      next: [route]
      on_fail: [bad]
    - id: route
      type: condition
      match: all
      branches:
        - {when: "data.amount > 1000", next: [vip, notify]}
      otherwise: [app]
    - id: app
      type: call
      target: {url: "http://app/process"}
      next: [done]
      on_reject: [bad]
    - id: vip
      type: topic
      topic: orders.vip
      next: [app]
    - id: notify
      type: webhook
      url: http://crm/notify
    - id: done
      type: topic
      topic: orders.processed
    - id: bad
      type: reject
`

func parseFlowPipeline(t *testing.T, src string) Pipeline {
	t.Helper()
	var p Pipeline
	if err := yaml.Unmarshal([]byte(src), &p); err != nil {
		t.Fatal(err)
	}
	ApplyPipelineDefaults(&p)
	return p
}

func TestValidFlowPasses(t *testing.T) {
	p := parseFlowPipeline(t, flowPipeline)
	if err := ValidatePipelines([]Pipeline{p}); err != nil {
		t.Fatalf("expected a valid flow, got %v", err)
	}
	app, _ := p.Flow.Step("app")
	if app.Target.TimeoutMs != 30000 || app.Retry.MaxAttempts != 3 || app.CircuitBreaker.FailureThreshold != 5 {
		t.Fatalf("expected call step defaults from the pipeline, got %+v %+v %+v", app.Target, app.Retry, app.CircuitBreaker)
	}
}

func TestFlowRejectsLoops(t *testing.T) {
	p := parseFlowPipeline(t, strings.Replace(flowPipeline, "next: [done]", "next: [route]", 1))
	err := ValidatePipelines([]Pipeline{p})
	if err == nil || !strings.Contains(err.Error(), "loop") {
		t.Fatalf("expected a loop error, got %v", err)
	}
}

func TestFlowRejectsUnknownSteps(t *testing.T) {
	p := parseFlowPipeline(t, strings.Replace(flowPipeline, "next: [done]", "next: [nowhere]", 1))
	err := ValidatePipelines([]Pipeline{p})
	if err == nil || !strings.Contains(err.Error(), `unknown step "nowhere"`) {
		t.Fatalf("expected an unknown step error, got %v", err)
	}
}

func TestFlowTerminalStepsCannotContinue(t *testing.T) {
	p := parseFlowPipeline(t, strings.Replace(flowPipeline, "      type: reject\n", "      type: reject\n      next: [done]\n", 1))
	err := ValidatePipelines([]Pipeline{p})
	if err == nil || !strings.Contains(err.Error(), "ends the path") {
		t.Fatalf("expected a terminal step error, got %v", err)
	}
}

func TestFlowCallNeedsAFailurePath(t *testing.T) {
	p := parseFlowPipeline(t, strings.Replace(flowPipeline, "dead_letter_topic: orders.dlq\n", "", 1))
	err := ValidatePipelines([]Pipeline{p})
	if err == nil || !strings.Contains(err.Error(), "on_failure") {
		t.Fatalf("expected a missing failure path error, got %v", err)
	}
}
