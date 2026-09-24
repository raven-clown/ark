package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing test config: %v", err)
	}
	return path
}

func TestLoadValidConfig(t *testing.T) {
	path := writeConfig(t, `
brokers:
  - localhost:9092
pipelines:
  - name: order-processor
    source_topic: orders.raw
    destination_topic: orders.processed
    consumer_group: order-processor-group
    target:
      url: http://app:8080/process
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.Pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(cfg.Pipelines))
	}

	p := cfg.Pipelines[0]
	if p.MCPAccess != MCPAccessReadOnly {
		t.Errorf("expected default mcp_access read_only, got %q", p.MCPAccess)
	}
	if p.Target.Mode != TargetModeSingleURL {
		t.Errorf("expected default target.mode single_url, got %q", p.Target.Mode)
	}
	if p.Workers != 1 {
		t.Errorf("expected default workers 1, got %d", p.Workers)
	}
	if p.Retry.MaxAttempts != 3 {
		t.Errorf("expected default retry.max_attempts 3, got %d", p.Retry.MaxAttempts)
	}
	if p.Concurrency.MaxInFlight != 10 {
		t.Errorf("expected default concurrency.max_in_flight 10, got %d", p.Concurrency.MaxInFlight)
	}
}

func TestLoadMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		yaml   string
		errSub string
	}{
		{
			name:   "no brokers",
			yaml:   "pipelines: []\n",
			errSub: "brokers",
		},
		{
			name: "no pipeline name",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - source_topic: orders.raw
`,
			errSub: "name is required",
		},
		{
			name: "no source_topic",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - name: p1
`,
			errSub: "source_topic is required",
		},
		{
			name: "enabled pipeline missing destination_topic",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: orders.raw
    consumer_group: g1
    target: {url: http://app/process}
`,
			errSub: "destination_topic is required",
		},
		{
			name: "enabled pipeline missing target.url",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: orders.raw
    destination_topic: orders.processed
    consumer_group: g1
`,
			errSub: "target.url is required",
		},
		{
			name: "enabled pipeline missing consumer_group",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: orders.raw
    destination_topic: orders.processed
    target: {url: http://app/process}
`,
			errSub: "consumer_group is required",
		},
		{
			name: "duplicate pipeline name",
			yaml: `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: a
    enabled: false
  - name: p1
    source_topic: b
    enabled: false
`,
			errSub: "duplicate pipeline name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.yaml)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSub)
			}
			if !strings.Contains(err.Error(), tc.errSub) {
				t.Errorf("expected error containing %q, got %q", tc.errSub, err.Error())
			}
		})
	}
}

func TestDisabledPipelineSkipsRuntimeValidation(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: draft-pipeline
    source_topic: orders.raw
    enabled: false
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected draft pipeline to load without destination_topic/target/consumer_group, got error: %v", err)
	}
	if cfg.Pipelines[0].IsEnabled() {
		t.Errorf("expected pipeline to be disabled")
	}
}

func TestClusterDefaults(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
cluster:
  enabled: true
pipelines:
  - name: draft-pipeline
    source_topic: orders.raw
    enabled: false
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Cluster.HeartbeatIntervalSeconds != 5 {
		t.Errorf("expected default heartbeat_interval_seconds 5, got %d", cfg.Cluster.HeartbeatIntervalSeconds)
	}
	if cfg.Cluster.NodeTimeoutSeconds != 20 {
		t.Errorf("expected default node_timeout_seconds 20, got %d", cfg.Cluster.NodeTimeoutSeconds)
	}
	if cfg.Cluster.PlacementIntervalSeconds != 5 {
		t.Errorf("expected default placement_interval_seconds 5, got %d", cfg.Cluster.PlacementIntervalSeconds)
	}
}

func TestClusterDisabledSkipsDefaults(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: draft-pipeline
    source_topic: orders.raw
    enabled: false
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Cluster.HeartbeatIntervalSeconds != 0 {
		t.Errorf("expected no defaults applied when cluster.enabled is false, got heartbeat_interval_seconds %d", cfg.Cluster.HeartbeatIntervalSeconds)
	}
}

func TestClusterNodeTimeoutMustExceedHeartbeatInterval(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
cluster:
  enabled: true
  heartbeat_interval_seconds: 10
  node_timeout_seconds: 5
pipelines:
  - name: draft-pipeline
    source_topic: orders.raw
    enabled: false
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when node_timeout_seconds <= heartbeat_interval_seconds")
	}
	if !strings.Contains(err.Error(), "node_timeout_seconds") {
		t.Errorf("expected error to mention node_timeout_seconds, got: %v", err)
	}
}

func TestTransformRouteRequiresExactlyOneOverride(t *testing.T) {
	base := `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: a
    destination_topic: b
    consumer_group: g1
    target: {url: http://app/process}
    fast_path_rules:
      - name: r1
        condition: "data.x == 1"
        action: transform_route
%s
`

	t.Run("neither override set", func(t *testing.T) {
		path := writeConfig(t, fmt.Sprintf(base, ""))
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "requires destination_override or webhook_override") {
			t.Fatalf("expected requires-override error, got %v", err)
		}
	})

	t.Run("both overrides set", func(t *testing.T) {
		path := writeConfig(t, fmt.Sprintf(base, "        destination_override: topic.b\n        webhook_override: http://other/hook\n"))
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "not both") {
			t.Fatalf("expected not-both error, got %v", err)
		}
	})

	t.Run("exactly one override set", func(t *testing.T) {
		path := writeConfig(t, fmt.Sprintf(base, "        destination_override: topic.b\n"))
		if _, err := Load(path); err != nil {
			t.Fatalf("expected valid config, got error: %v", err)
		}
	})
}

func TestUnknownRuleActionRejected(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: a
    destination_topic: b
    consumer_group: g1
    target: {url: http://app/process}
    fast_path_rules:
      - name: r1
        condition: "data.x == 1"
        action: do_something_unsupported
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unknown action") {
		t.Fatalf("expected unknown action error, got %v", err)
	}
}

func TestRuleWithoutConditionRejected(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: p1
    source_topic: a
    destination_topic: b
    consumer_group: g1
    target: {url: http://app/process}
    fast_path_rules:
      - name: r1
        action: drop
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "condition is required") {
		t.Fatalf("expected condition-required error, got %v", err)
	}
}

func TestIsRejectDefaultsExcludeRetryableStatuses(t *testing.T) {
	var target Target
	for _, s := range []int{408, 425, 429, 500, 503, 200} {
		if target.IsReject(s) {
			t.Errorf("status %d must not be a reject by default", s)
		}
	}
	for _, s := range []int{400, 404, 422} {
		if !target.IsReject(s) {
			t.Errorf("status %d should be a reject by default", s)
		}
	}
}

func TestIsRejectHonorsConfiguredStatuses(t *testing.T) {
	target := Target{RejectStatuses: []int{422}}
	if target.IsReject(400) {
		t.Error("400 is not in reject_statuses, so it must not be a reject")
	}
	if !target.IsReject(422) {
		t.Error("422 is in reject_statuses")
	}
}

func TestUnknownOrderingRejected(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: p
    source_topic: a
    destination_topic: b
    consumer_group: g
    ordering: sometimes
    target:
      url: http://x
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "ordering") {
		t.Fatalf("expected an ordering validation error, got %v", err)
	}
}

func TestDefaultsForOrderingTimeoutAndReplication(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
pipelines:
  - name: p
    source_topic: a
    destination_topic: b
    consumer_group: g
    target:
      url: http://x
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Pipelines[0]
	if p.Ordering != OrderingPerKey || p.Target.TimeoutMs != 30000 || cfg.Topics.ReplicationFactor != 3 {
		t.Errorf("unexpected defaults: ordering=%q timeout_ms=%d replication_factor=%d", p.Ordering, p.Target.TimeoutMs, cfg.Topics.ReplicationFactor)
	}
}

func TestClusterNameMustBeTopicSafe(t *testing.T) {
	path := writeConfig(t, `
brokers: [localhost:9092]
cluster:
  enabled: true
  name: "Prod East"
`)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "cluster.name") {
		t.Fatalf("expected a cluster.name validation error, got %v", err)
	}
}

func TestRedriveNeedsDLQAndPositiveValues(t *testing.T) {
	base := `
brokers: [localhost:9092]
pipelines:
  - name: p
    source_topic: a
    destination_topic: b
    consumer_group: g
    target: {url: http://x}
`
	for _, extra := range []string{
		"    dead_letter_redrive: {after_seconds: 60, max_times: 3}\n",
		"    dead_letter_topic: d\n    dead_letter_redrive: {after_seconds: 0, max_times: 3}\n",
	} {
		if _, err := Load(writeConfig(t, base+extra)); err == nil || !strings.Contains(err.Error(), "dead_letter_redrive") {
			t.Errorf("expected a dead_letter_redrive error for %q, got %v", extra, err)
		}
	}
	if _, err := Load(writeConfig(t, base+"    dead_letter_topic: d\n    dead_letter_redrive: {after_seconds: 60, max_times: 3}\n")); err != nil {
		t.Errorf("valid redrive config rejected: %v", err)
	}
}
