package datarules

import (
	"strings"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func f(v float64) *float64 { return &v }

func checker(t *testing.T, r config.DataRules) *Checker {
	t.Helper()
	c, err := New(r)
	if err != nil || c == nil {
		t.Fatalf("New: %v %v", c, err)
	}
	return c
}

func rulesOf(vs []Violation) string {
	var s []string
	for _, v := range vs {
		s = append(s, v.Rule)
	}
	return strings.Join(s, ",")
}

func TestValidMessagePasses(t *testing.T) {
	c := checker(t, config.DataRules{Fields: []config.FieldRule{
		{Path: "order_id", Required: true, Type: "string", Pattern: `^ORD-\d+$`},
		{Path: "amount", Required: true, Type: "number", Min: f(0), Max: f(100000)},
		{Path: "currency", Enum: []string{"THB", "USD"}},
		{Path: "customer.email", Format: "email"},
		{Path: "created_at", Format: "date-time"},
	}})
	msg := `{"order_id":"ORD-17","amount":250.5,"currency":"THB","customer":{"email":"a@b.co"},"created_at":"2026-09-24T21:05:00+07:00"}`
	if vs := c.Check(nil, nil, []byte(msg)); len(vs) != 0 {
		t.Fatalf("expected no violations, got %+v", vs)
	}
}

func TestEachKindOfViolation(t *testing.T) {
	c := checker(t, config.DataRules{Fields: []config.FieldRule{
		{Path: "order_id", Required: true, Type: "string", Pattern: `^ORD-\d+$`},
		{Path: "amount", Type: "number", Min: f(0)},
		{Path: "qty", Type: "integer"},
		{Path: "currency", Enum: []string{"THB", "USD"}},
		{Path: "customer.email", Format: "email"},
	}})
	msg := `{"order_id":"17","amount":-5,"qty":1.5,"currency":"XXX","customer":{"email":"not-an-email"}}`
	got := rulesOf(c.Check(nil, nil, []byte(msg)))
	for _, want := range []string{"pattern", "min", "type", "enum", "format"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected a %s violation, got %s", want, got)
		}
	}
	if got := rulesOf(c.Check(nil, nil, []byte(`{"amount":1}`))); !strings.Contains(got, "required") {
		t.Errorf("missing order_id should be required violation, got %s", got)
	}
}

func TestNotJSONAndUnknownFieldsAndKeyHeaders(t *testing.T) {
	no := false
	c := checker(t, config.DataRules{
		AllowUnknownFields: &no,
		MaxBytes:           200,
		Key:                &config.ValueRule{Required: true, Pattern: `^[a-z0-9-]+$`},
		Headers:            []config.HeaderRule{{Name: "source", ValueRule: config.ValueRule{Required: true, Enum: []string{"web", "app"}}}},
		Fields:             []config.FieldRule{{Path: "id"}},
	})
	if got := rulesOf(c.Check([]byte("k1"), map[string]string{"source": "web"}, []byte("not json"))); !strings.Contains(got, "json") {
		t.Errorf("expected json violation, got %s", got)
	}
	got := rulesOf(c.Check([]byte("BAD KEY"), map[string]string{"source": "fax"}, []byte(`{"id":1,"surprise":true}`)))
	for _, want := range []string{"pattern", "enum", "allow_unknown_fields"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %s, got %s", want, got)
		}
	}
	if got := rulesOf(c.Check(nil, nil, []byte(`{"id":1}`))); !strings.Contains(got, "required") {
		t.Errorf("missing key and header should be required violations, got %s", got)
	}
	big := `{"id":"` + strings.Repeat("x", 300) + `"}`
	if got := rulesOf(c.Check([]byte("k"), map[string]string{"source": "web"}, []byte(big))); !strings.Contains(got, "max_bytes") {
		t.Errorf("expected max_bytes, got %s", got)
	}
}

func TestNoRulesMeansNoChecker(t *testing.T) {
	c, err := New(config.DataRules{})
	if err != nil || c != nil {
		t.Fatalf("empty rules should give a nil checker, got %v %v", c, err)
	}
}
