package consumer

import (
	"bytes"
	"testing"
	"text/template"
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
