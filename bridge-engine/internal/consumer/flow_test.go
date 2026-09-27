package consumer

import (
	"bytes"
	"testing"
	"text/template"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestExpandHeadersReadsTheEnvironment(t *testing.T) {
	t.Setenv("ARK_TEST_TOKEN", "s3cret")
	got := expandHeaders(map[string]string{"Authorization": "Bearer ${ARK_TEST_TOKEN}", "X-Plain": "$5"})
	if got["Authorization"] != "Bearer s3cret" || got["X-Plain"] != "$5" {
		t.Fatalf("expected only ${NAME} expanded, got %v", got)
	}
}

func TestWebhookBodyTemplate(t *testing.T) {
	tmpl := template.Must(template.New("b").Funcs(template.FuncMap{"json": toJSON}).Option("missingkey=zero").Parse(`{"text": {{json (printf "Order %v rejected: %s" .data.order_id .reason)}}}`))
	m := flowMsg{value: []byte(`{"order_id":"A1"}`), headers: map[string]string{}, reason: `amount is -5 "low"`}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, m.env()); err != nil {
		t.Fatal(err)
	}
	want := `{"text": "Order A1 rejected: amount is -5 \"low\""}`
	if buf.String() != want {
		t.Fatalf("got %s, want %s", buf.String(), want)
	}
}

func TestWebhookURLTemplateKeepsTheHost(t *testing.T) {
	compile := func(u string) (*flowRuntime, config.Step) {
		s := config.Step{ID: "s", URL: u}
		return &flowRuntime{urls: map[string]*template.Template{"s": template.Must(template.New("s").Funcs(templateFuncs).Option("missingkey=zero").Parse(u))}}, s
	}
	rt, s := compile("http://opensearch:9200/orders/_doc/{{path .data.order_id}}")
	got, err := rt.webhookURL(s, flowMsg{value: []byte(`{"order_id":"A 1/../x"}`), headers: map[string]string{}})
	if err != nil || got != "http://opensearch:9200/orders/_doc/A%201%2F..%2Fx" {
		t.Fatalf("got %q, %v", got, err)
	}

	if _, err := rt.webhookURL(s, flowMsg{value: []byte(`{"id":"A1"}`), headers: map[string]string{}}); err == nil {
		t.Fatal("a missing id must fail instead of sending to /<nil>")
	}

	rt, s = compile("http://app:8080/{{.data.next}}")
	if _, err := rt.webhookURL(s, flowMsg{value: []byte(`{"next":"@evil:80/x"}`), headers: map[string]string{}}); err != nil {
		t.Fatalf("a path that only looks like a host should pass: %v", err)
	}
	rt, s = compile("http://app:8080?to={{.data.next}}")
	if _, err := rt.webhookURL(s, flowMsg{value: []byte(`{"next":"x"}`), headers: map[string]string{}}); err != nil {
		t.Fatalf("query template: %v", err)
	}
}
