package mcpserver

import (
	"context"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type staticSource struct{ p []config.Pipeline }

func (s staticSource) Pipelines() []config.Pipeline                   { return s.p }
func (s staticSource) Apply(context.Context, []config.Pipeline) error { return nil }
func (s staticSource) Mode() string                                   { return "file" }

func testDeps() Deps {
	return Deps{Config: staticSource{p: []config.Pipeline{
		{Name: "order-processor", SourceTopic: "orders.raw", MCPAccess: config.MCPAccessReadWrite},
		{Name: "payment-sync", SourceTopic: "payments.raw", MCPAccess: config.MCPAccessReadOnly},
		{Name: "secret-audit", SourceTopic: "audit.raw", MCPAccess: config.MCPAccessNone},
	}}}
}

func hasIntent(o interpretOut, intent string) bool {
	for _, i := range o.Intents {
		if i.Intent == intent {
			return true
		}
	}
	return false
}

func TestInterpretAcrossLanguages(t *testing.T) {
	d := testDeps()
	cases := []struct {
		text, intent, pipeline string
	}{
		{"why is orders so slow?", "diagnose", "order-processor"},
		{"ทำไม orders ช้าจัง", "diagnose", "order-processor"},
		{"为什么 orders 这么慢", "diagnose", "order-processor"},
		{"為什麼 payment 卡住了", "diagnose", "payment-sync"},
		{"สวัสดีครับ", "greeting", ""},
		{"你好", "greeting", ""},
		{"เมื่อคืนเกิดอะไรขึ้นกับ payment-sync", "events", "payment-sync"},
	}
	for _, c := range cases {
		o := interpret(d, c.text)
		if !hasIntent(o, c.intent) {
			t.Errorf("%q: expected intent %s, got %+v", c.text, c.intent, o.Intents)
		}
		if c.pipeline != "" && (len(o.Pipelines) == 0 || o.Pipelines[0].Pipeline != c.pipeline) {
			t.Errorf("%q: expected pipeline %s, got %+v", c.text, c.pipeline, o.Pipelines)
		}
		if len(o.Plan) == 0 {
			t.Errorf("%q: expected a plan", c.text)
		}
	}
}

func TestInterpretRatesTimesAndHiddenPipelines(t *testing.T) {
	d := testDeps()
	if o := interpret(d, "ตั้งค่า order-processor ให้รับ 2k msg/s"); o.RatePerSec != 2000 {
		t.Errorf("expected 2000 msg/s, got %v", o.RatePerSec)
	}
	if o := interpret(d, "order-processor 每秒3千条 需要怎么配置"); o.RatePerSec != 3000 {
		t.Errorf("expected 3000/s from Chinese, got %v", o.RatePerSec)
	}
	if o := interpret(d, "what happened last night?"); o.SinceMinutes == 0 {
		t.Error("expected a time window for 'last night'")
	}
	if o := interpret(d, "why is secret-audit slow"); len(o.Pipelines) != 0 {
		t.Errorf("a pipeline with mcp_access: none must never be matched, got %+v", o.Pipelines)
	}
	if o := interpret(d, "create a pipeline for new signups"); len(o.Clarifications) == 0 {
		t.Error("creating without a URL or topic should ask the user for them")
	}
}

func TestConfirmTokenIsSingleUseAndBound(t *testing.T) {
	cf := newConfirmations()
	tok := cf.put(pendingChange{action: "create_pipeline", userID: "alice", expires: farFuture()})
	if _, err := cf.take(tok, "mallory"); err == nil {
		t.Fatal("another caller must not be able to use the token")
	}
	tok = cf.put(pendingChange{action: "create_pipeline", userID: "alice", expires: farFuture()})
	if _, err := cf.take(tok, "alice"); err != nil {
		t.Fatalf("the caller who previewed should confirm: %v", err)
	}
	if _, err := cf.take(tok, "alice"); err == nil {
		t.Fatal("a token must only work once")
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	if _, err := parsePipelineYAML("name: x\nsource_topik: typo\n"); err == nil {
		t.Fatal("a misspelled field must be rejected, not silently ignored")
	}
}

func farFuture() time.Time { return time.Now().Add(time.Hour) }
