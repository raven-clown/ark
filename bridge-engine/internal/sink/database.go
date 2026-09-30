package sink

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/template" // nosemgrep: go.lang.security.audit.xss.import-text-template.import-text-template -- renders query parameters, never SQL text or HTML
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type Database struct {
	db     *sql.DB
	query  string
	cols   []string
	tmpls  []*template.Template
	broken error
}

func NewDatabase(d config.Database, funcs template.FuncMap) (*Database, error) {
	out := &Database{}
	for col := range d.Columns {
		if !config.SQLIdent(col, false) {
			return nil, fmt.Errorf("column %q is not a plain name", col)
		}
		out.cols = append(out.cols, col)
	}
	if !config.SQLIdent(d.Table, true) {
		return nil, fmt.Errorf("table %q is not a plain name", d.Table)
	}
	sort.Strings(out.cols)
	for _, col := range out.cols {
		t, err := template.New(col).Funcs(funcs).Option("missingkey=zero").Parse(d.Columns[col])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col, err)
		}
		out.tmpls = append(out.tmpls, t)
	}
	out.query = insertQuery(d.Table, out.cols, d.UpsertOn)
	dsn := os.Getenv(d.DSNEnv)
	if dsn == "" {
		out.broken = fmt.Errorf("environment variable %s (database.dsn_env) is empty on this ARK server", d.DSNEnv)
		return out, nil
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening the database: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(5 * time.Minute)
	out.db = db
	return out, nil
}

func quote(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

func insertQuery(table string, cols, upsertOn []string) string {
	names := make([]string, len(cols))
	params := make([]string, len(cols))
	for i, c := range cols {
		names[i], params[i] = quote(c), "$"+strconv.Itoa(i+1)
	}
	q := "INSERT INTO " + quote(table) + " (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(params, ", ") + ")"
	if len(upsertOn) == 0 {
		return q
	}
	key := map[string]bool{}
	conflict := make([]string, len(upsertOn))
	for i, c := range upsertOn {
		key[c], conflict[i] = true, quote(c)
	}
	var set []string
	for _, c := range cols {
		if !key[c] {
			set = append(set, quote(c)+" = EXCLUDED."+quote(c))
		}
	}
	q += " ON CONFLICT (" + strings.Join(conflict, ", ") + ")"
	if len(set) == 0 {
		return q + " DO NOTHING"
	}
	return q + " DO UPDATE SET " + strings.Join(set, ", ")
}

func (d *Database) values(m Message) ([]any, error) {
	args := make([]any, len(d.tmpls))
	for i, t := range d.tmpls {
		var buf bytes.Buffer
		if err := t.Execute(&buf, m.Env); err != nil {
			return nil, fmt.Errorf("column %s: %w", d.cols[i], err)
		}
		if s := buf.String(); s != "" && s != "<no value>" {
			args[i] = s
		}
	}
	return args, nil
}

func (d *Database) Write(ctx context.Context, m Message) error {
	if d.broken != nil {
		return d.broken
	}
	args, err := d.values(m)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := d.db.ExecContext(ctx, d.query, args...); err != nil {
		return fmt.Errorf("writing to the database: %w", err)
	}
	return nil
}

func (d *Database) Close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}
