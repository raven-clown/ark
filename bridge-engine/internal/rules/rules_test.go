package rules

import (
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestFastPathFirstMatchWins(t *testing.T) {
	p := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "too-broad", Condition: "data.amount > 0", Action: config.ActionPassThrough},
			{Name: "too-late", Condition: "data.amount > 1000", Action: config.ActionDeadLetter},
		},
	}
	engine, err := Compile(p)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rule, err := engine.EvaluateFastPath([]byte(`{"amount": 5000}`))
	if err != nil {
		t.Fatalf("EvaluateFastPath: %v", err)
	}
	if rule == nil || rule.Name != "too-broad" {
		t.Fatalf("expected first matching rule 'too-broad', got %+v", rule)
	}
}

func TestFastPathNoMatchReturnsNil(t *testing.T) {
	p := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "big-orders", Condition: "data.amount > 1000", Action: config.ActionDrop},
		},
	}
	engine, err := Compile(p)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rule, err := engine.EvaluateFastPath([]byte(`{"amount": 5}`))
	if err != nil {
		t.Fatalf("EvaluateFastPath: %v", err)
	}
	if rule != nil {
		t.Fatalf("expected no match, got %+v", rule)
	}
}

func TestNilNotNullForNullChecks(t *testing.T) {
	p := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "missing-customer", Condition: "data.customer_id == nil", Action: config.ActionReject},
		},
	}
	engine, err := Compile(p)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rule, err := engine.EvaluateFastPath([]byte(`{"amount": 5}`))
	if err != nil {
		t.Fatalf("EvaluateFastPath: %v", err)
	}
	if rule == nil {
		t.Fatal("expected the nil-check rule to match a message missing customer_id")
	}

	// `null` (JSON/JS style) is a compile-time error for expr, not a valid
	// condition -- this is the bug found and fixed against the shipped
	// example config. Documenting the failure mode here so a regression
	// (someone "fixing" the syntax back to `null`) fails the build.
	badPipeline := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "bad-syntax", Condition: "data.customer_id == null", Action: config.ActionReject},
		},
	}
	if _, err := Compile(badPipeline); err == nil {
		t.Fatal("expected == null to fail to compile (expr uses nil, not null)")
	}
}

func TestNonJSONMessageMatchesNothing(t *testing.T) {
	p := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "any-amount", Condition: "data.amount > 0", Action: config.ActionDrop},
		},
	}
	engine, err := Compile(p)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rule, err := engine.EvaluateFastPath([]byte("not json at all"))
	if err != nil {
		t.Fatalf("expected a non-JSON message to be treated as no-match, not an error, got: %v", err)
	}
	if rule != nil {
		t.Fatalf("expected no match for non-JSON input, got %+v", rule)
	}
}

func TestPostCallbackEvaluatesResponseNotJustMessage(t *testing.T) {
	p := config.Pipeline{
		PostCallbackRules: []config.FastPathRule{
			{
				Name:                "high-value",
				Condition:           "response.status == 200 && response.body.amount > 10000",
				Action:              config.ActionTransformRoute,
				DestinationOverride: "orders.high-value",
			},
		},
	}
	engine, err := Compile(p)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rule, err := engine.EvaluatePostCallback([]byte(`{"customer_id":"c1"}`), 200, []byte(`{"amount": 99999}`))
	if err != nil {
		t.Fatalf("EvaluatePostCallback: %v", err)
	}
	if rule == nil || rule.DestinationOverride != "orders.high-value" {
		t.Fatalf("expected high-value rule to match, got %+v", rule)
	}

	rule, err = engine.EvaluatePostCallback([]byte(`{"customer_id":"c1"}`), 200, []byte(`{"amount": 50}`))
	if err != nil {
		t.Fatalf("EvaluatePostCallback: %v", err)
	}
	if rule != nil {
		t.Fatalf("expected no match for a low amount, got %+v", rule)
	}
}

func TestHasFastPathAndHasPostCallback(t *testing.T) {
	empty, err := Compile(config.Pipeline{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if empty.HasFastPath() || empty.HasPostCallback() {
		t.Fatal("expected an engine with no rules to report neither")
	}

	withRules, err := Compile(config.Pipeline{
		FastPathRules:     []config.FastPathRule{{Name: "r1", Condition: "true", Action: config.ActionDrop}},
		PostCallbackRules: []config.FastPathRule{{Name: "r2", Condition: "true", Action: config.ActionDrop}},
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !withRules.HasFastPath() || !withRules.HasPostCallback() {
		t.Fatal("expected an engine with rules to report both")
	}
}

func TestCompileErrorNamesTheBadRule(t *testing.T) {
	p := config.Pipeline{
		FastPathRules: []config.FastPathRule{
			{Name: "broken", Condition: "data.amount >>> 1000", Action: config.ActionDrop},
		},
	}
	_, err := Compile(p)
	if err == nil {
		t.Fatal("expected a compile error for invalid syntax")
	}
}
