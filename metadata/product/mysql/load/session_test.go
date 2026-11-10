package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
)

func TestLoadDiagnostics(t *testing.T) {
	note := []driver.Value{"Note", int64(1265), "fraction rounded"}
	warning := []driver.Value{"Warning", int64(1062), "duplicate key"}
	failure := errors.New("diagnostic query failed")
	for _, test := range []struct {
		name        string
		total       driver.Value
		diagnostics [][]driver.Value
		countErr    error
		queryErr    error
		iterateErr  error
		wantError   string
	}{
		{name: "no diagnostics", total: int64(0)},
		{name: "rounding note", total: int64(1), diagnostics: [][]driver.Value{note}},
		{name: "multiple notes", total: int64(2), diagnostics: [][]driver.Value{note, note}},
		{name: "warning", total: int64(1), diagnostics: [][]driver.Value{warning}, wantError: "Warning 1062"},
		{name: "warning after note", total: int64(2), diagnostics: [][]driver.Value{note, warning}, wantError: "Warning 1062"},
		{name: "error", total: int64(1), diagnostics: [][]driver.Value{{"Error", int64(1), "error"}}, wantError: "Error 1"},
		{name: "unknown severity", total: int64(1), diagnostics: [][]driver.Value{{"unknown", int64(1), "error"}}, wantError: "unknown 1"},
		{name: "truncated after note", total: int64(2), diagnostics: [][]driver.Value{note}, wantError: "received 1 of 2"},
		{name: "storage disabled", total: int64(1), wantError: "received 0 of 1"},
		{name: "count query error", countErr: failure, wantError: failure.Error()},
		{name: "count scan error", total: "invalid", wantError: "Scan error"},
		{name: "warnings query error", total: int64(1), queryErr: failure, wantError: failure.Error()},
		{name: "warnings scan error", total: int64(1), diagnostics: [][]driver.Value{{"Note", "invalid", "error"}}, wantError: "Scan error"},
		{name: "iteration error", total: int64(2), diagnostics: [][]driver.Value{note}, iterateErr: failure, wantError: failure.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &diagnosticState{total: test.total, diagnostics: test.diagnostics, countErr: test.countErr, queryErr: test.queryErr, iterateErr: test.iterateErr}
			db := sql.OpenDB(&diagnosticConnector{state: state})
			defer db.Close()
			session := NewSession(&info.Dialect{Transactional: true})
			_, err := session.Exec(context.Background(), []struct{ ID int }{{1}}, db, "test_load")
			if test.wantError == "" {
				if err != nil || state.commits != 1 || state.rollbacks != 0 {
					t.Fatalf("err=%v commits=%d rollbacks=%d", err, state.commits, state.rollbacks)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) || state.commits != 0 || state.rollbacks != 1 {
				t.Fatalf("err=%v, want %q; commits=%d rollbacks=%d", err, test.wantError, state.commits, state.rollbacks)
			}
			if test.countErr == failure || test.queryErr == failure || test.iterateErr == failure {
				if !errors.Is(err, failure) {
					t.Fatalf("lost diagnostic error: %v", err)
				}
			}
			if state.openRows != 0 {
				t.Fatalf("unclosed diagnostic results: %d", state.openRows)
			}
			if state.connections != 1 {
				t.Fatalf("load and diagnostics used %d connections", state.connections)
			}
			if test.countErr == nil && test.wantError != "Scan error" {
				if !reflect.DeepEqual(state.queries, []string{"LOAD", "SHOW COUNT(*) WARNINGS", "SHOW WARNINGS"}) {
					t.Fatalf("query order: %v", state.queries)
				}
			}
		})
	}
}

func TestLoadDiagnosticsCallerTransaction(t *testing.T) {
	state := &diagnosticState{total: int64(1)}
	db := sql.OpenDB(&diagnosticConnector{state: state})
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = NewSession(&info.Dialect{Transactional: true}).Exec(context.Background(), []struct{ ID int }{{1}}, db, "test_load", loption.WithTransaction(tx))
	if err == nil || !strings.Contains(err.Error(), "diagnostics incomplete") {
		t.Fatalf("expected incomplete diagnostics error, got %v", err)
	}
	if state.commits != 0 || state.rollbacks != 0 {
		t.Fatal("loader ended caller transaction")
	}
	if err := tx.Rollback(); err != nil || state.rollbacks != 1 {
		t.Fatalf("caller rollback: %v", err)
	}
}

func TestLoadDiagnosticsPinnedNontransactionalConnection(t *testing.T) {
	state := &diagnosticState{total: int64(0)}
	db := sql.OpenDB(&diagnosticConnector{state: state})
	defer db.Close()
	// Returning a connection to the pool would close it and lose its diagnostics.
	db.SetMaxIdleConns(0)
	_, err := NewSession(&info.Dialect{}).Exec(context.Background(), []struct{ ID int }{{1}}, db, "test_load")
	if err != nil || state.connections != 1 {
		t.Fatalf("err=%v connections=%d", err, state.connections)
	}
}

type diagnosticState struct {
	total                                     driver.Value
	diagnostics                               [][]driver.Value
	countErr, queryErr, iterateErr            error
	commits, rollbacks, connections, openRows int
	queries                                   []string
}

type diagnosticConnector struct{ state *diagnosticState }
type diagnosticDriver struct{}
type diagnosticConn struct{ state *diagnosticState }
type diagnosticTx struct{ state *diagnosticState }
type diagnosticRows struct {
	state   *diagnosticState
	columns []string
	values  [][]driver.Value
	err     error
	closed  bool
}

func (c *diagnosticConnector) Connect(context.Context) (driver.Conn, error) {
	c.state.connections++
	return &diagnosticConn{state: c.state}, nil
}
func (*diagnosticConnector) Driver() driver.Driver        { return diagnosticDriver{} }
func (diagnosticDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (*diagnosticConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*diagnosticConn) Close() error                { return nil }
func (c *diagnosticConn) Begin() (driver.Tx, error) { return &diagnosticTx{state: c.state}, nil }
func (t *diagnosticTx) Commit() error               { t.state.commits++; return nil }
func (t *diagnosticTx) Rollback() error             { t.state.rollbacks++; return nil }
func (c *diagnosticConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(query, "LOAD DATA") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	c.state.queries = append(c.state.queries, "LOAD")
	return driver.RowsAffected(1), nil
}
func (c *diagnosticConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.queries = append(c.state.queries, query)
	rows := &diagnosticRows{state: c.state}
	switch query {
	case "SHOW COUNT(*) WARNINGS":
		if c.state.countErr != nil {
			return nil, c.state.countErr
		}
		rows.columns = []string{"@@session.warning_count"}
		rows.values = [][]driver.Value{{c.state.total}}
	case "SHOW WARNINGS":
		if c.state.queryErr != nil {
			return nil, c.state.queryErr
		}
		rows.columns = []string{"Level", "Code", "Message"}
		rows.values = c.state.diagnostics
		rows.err = c.state.iterateErr
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	c.state.openRows++
	return rows, nil
}
func (r *diagnosticRows) Columns() []string { return r.columns }
func (r *diagnosticRows) Close() error {
	if !r.closed {
		r.state.openRows--
		r.closed = true
	}
	return nil
}
func (r *diagnosticRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}
