package mcpserver

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestSchemaExamplesAreValid(t *testing.T) {
	for name, src := range map[string]string{"example": exampleYAML, "flow": flowExampleYAML} {
		var p config.Pipeline
		if err := yaml.Unmarshal([]byte(src), &p); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		config.ApplyPipelineDefaults(&p)
		if err := config.ValidatePipelines([]config.Pipeline{p}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestReviewOfAFlowTalksAboutSteps(t *testing.T) {
	var p config.Pipeline
	if err := yaml.Unmarshal([]byte(flowExampleYAML), &p); err != nil {
		t.Fatal(err)
	}
	config.ApplyPipelineDefaults(&p)
	var whats []string
	for _, f := range reviewConfig(Deps{}, p) {
		whats = append(whats, f.What)
	}
	got := strings.Join(whats, "|")
	if strings.Contains(got, "No reject_topic") || strings.Contains(got, "No target health check") {
		t.Errorf("fixed-path findings on a flow: %s", got)
	}
	if !strings.Contains(got, `Call step "fraud" has no health check URL.`) {
		t.Errorf("missing step finding: %s", got)
	}
}
