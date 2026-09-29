package mcpserver

import (
	"strings"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
)

func metricsDeps(t *testing.T) (Deps, *memSource) {
	t.Helper()
	tenanted := func(name, tenant string) config.Pipeline {
		return mustPipeline(t, pipelineYAML(name, name+".raw", name+".done")+"tenant: "+tenant+"\n")
	}
	hidden := tenanted("vault", "acme")
	hidden.MCPAccess = config.MCPAccessNone
	src := &memSource{p: []config.Pipeline{tenanted("orders", "acme"), hidden, tenanted("billing", "globex")}}
	d := Deps{Registry: api.NewRegistry(nil), Config: src, History: NewHistory()}
	all := d
	all.AllPipelines = true
	now := time.Now().Add(-time.Minute)
	for i := range 7 {
		lag := int64(i * 10)
		d.History.sampleWith(all, now.Add(time.Duration(i)*5*time.Second), map[string][]kafkaadmin.PartitionLag{
			"orders": {{Lag: lag}}, "vault": {{Lag: 1}}, "billing": {{Lag: 2}},
		})
	}
	return d, src
}

func TestGetMetricsForAPipelineSummarizesTheRange(t *testing.T) {
	d, _ := metricsDeps(t)
	out, err := getMetrics(d, metricsIn{Name: "orders", Minutes: 5})
	if err != nil {
		t.Fatal(err)
	}
	if out.Of != "pipeline orders" || len(out.Points) != 6 || out.EverySeconds != 5 {
		t.Fatalf("got %s with %d points every %ds", out.Of, len(out.Points), out.EverySeconds)
	}
	if out.Summary.LagNow != 60 || out.Summary.LagPeak != 60 {
		t.Fatalf("summary = %+v", out.Summary)
	}
	thinned, err := getMetrics(d, metricsIn{Name: "orders", Minutes: 5, MaxPoints: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(thinned.Points) != 2 || thinned.EverySeconds != 15 || thinned.Points[1].Lag != 60 {
		t.Fatalf("thinned to %d points every %ds: %+v", len(thinned.Points), thinned.EverySeconds, thinned.Points)
	}
}

func TestGetMetricsHonorsVisibility(t *testing.T) {
	d, _ := metricsDeps(t)
	if _, err := getMetrics(d, metricsIn{Name: "vault"}); err == nil {
		t.Fatal("a pipeline with mcp_access: none must not be readable")
	}
	if _, err := getMetrics(d, metricsIn{Tenant: "acme"}); err == nil || !strings.Contains(err.Error(), "can't see") {
		t.Fatalf("a tenant total that includes a hidden pipeline must be refused, got %v", err)
	}
	out, err := getMetrics(d, metricsIn{Tenant: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Of != "tenant globex" || len(out.Points) == 0 || out.Points[0].Lag != 2 {
		t.Fatalf("globex = %+v", out)
	}
}

func TestGetMetricsChecksItsInput(t *testing.T) {
	d, _ := metricsDeps(t)
	for _, in := range []metricsIn{
		{},
		{Name: "orders", Tenant: "acme"},
		{Name: "orders", From: "yesterday"},
		{Name: "orders", From: "2026-09-29T10:00:00Z", To: "2026-09-29T09:00:00Z"},
	} {
		if _, err := getMetrics(d, in); err == nil {
			t.Errorf("%+v should be refused", in)
		}
	}
	old, err := getMetrics(d, metricsIn{Name: "orders", From: time.Now().Add(-72 * time.Hour).Format(time.RFC3339)})
	if err != nil || old.Note == "" {
		t.Fatalf("a range older than the history should say so: %v %+v", err, old)
	}
}
