package sink

import (
	"context"
	"strings"
	"testing"
	"text/template"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestInsertQueryQuotesNamesAndUsesParameters(t *testing.T) {
	cases := []struct {
		table    string
		cols     []string
		upsertOn []string
		want     string
	}{
		{"orders", []string{"amount", "id"}, nil, `INSERT INTO "orders" ("amount", "id") VALUES ($1, $2)`},
		{"public.orders", []string{"amount", "id"}, []string{"id"}, `INSERT INTO "public"."orders" ("amount", "id") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "amount" = EXCLUDED."amount"`},
		{"seen", []string{"id"}, []string{"id"}, `INSERT INTO "seen" ("id") VALUES ($1) ON CONFLICT ("id") DO NOTHING`},
	}
	for _, c := range cases {
		if got := insertQuery(c.table, c.cols, c.upsertOn); got != c.want {
			t.Errorf("got  %s\nwant %s", got, c.want)
		}
	}
}

func TestDatabaseRendersColumnsAndMissingValuesAsNull(t *testing.T) {
	t.Setenv("ARK_TEST_DB", "postgres://u:p@127.0.0.1:1/x")
	d, err := NewDatabase(config.Database{Driver: "postgres", DSNEnv: "ARK_TEST_DB", Table: "orders",
		Columns: map[string]string{"id": "{{.data.id}}", "note": "{{.data.note}}", "who": "{{.key}}"}}, template.FuncMap{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	args, err := d.values(Message{Env: map[string]any{"data": map[string]any{"id": "A1"}, "key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 3 || args[0] != "A1" || args[1] != nil || args[2] != "k" {
		t.Fatalf("args = %#v", args)
	}
	missing, err := NewDatabase(config.Database{Driver: "postgres", DSNEnv: "ARK_MISSING_DB", Table: "t", Columns: map[string]string{"a": "1"}}, nil)
	if err != nil {
		t.Fatalf("an unset dsn_env must not stop the pipeline from starting: %v", err)
	}
	if err := missing.Write(context.Background(), Message{}); err == nil || !strings.Contains(err.Error(), "ARK_MISSING_DB") {
		t.Fatalf("writes with an unset dsn_env should fail and name it, got %v", err)
	}
	_ = missing.Close()
	if _, err := NewDatabase(config.Database{Driver: "postgres", DSNEnv: "ARK_TEST_DB", Table: `t"; drop table x; --`, Columns: map[string]string{"a": "1"}}, nil); err == nil {
		t.Fatal("a table name that isn't a plain identifier must be refused")
	}
}
