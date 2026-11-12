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
	"sync"
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
				if !reflect.DeepEqual(state.queries, []string{"SELECT @@session.sql_notes", "LOAD", "SHOW COUNT(*) WARNINGS", "SHOW WARNINGS"}) {
					t.Fatalf("query order: %v", state.queries)
				}
			}
		})
	}
}

func TestLoadDiagnosticsCallerTransaction(t *testing.T) {
	state := &diagnosticState{total: int64(1), notes: 1}
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
	if state.notes != 1 || !reflect.DeepEqual(state.queries, []string{"LOAD", "SHOW COUNT(*) WARNINGS", "SHOW WARNINGS"}) {
		t.Fatalf("loader modified caller settings: %v", state.queries)
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

func TestLoadNotesRestoration(t *testing.T) {
	loadFailure := errors.New("load failed")
	settingFailure := errors.New("setting failed")
	for _, test := range []struct {
		name                                      string
		notes                                     int
		loadErr, disableErr, restoreErr, beginErr error
		cancel                                    bool
	}{
		{name: "enabled notes restored before commit", notes: 1},
		{name: "disabled notes remain disabled"},
		{name: "load failure restores notes", notes: 1, loadErr: loadFailure},
		{name: "begin failure restores notes", notes: 1, beginErr: settingFailure},
		{name: "disable failure attempts restoration", notes: 1, disableErr: settingFailure},
		{name: "restore failure prevents commit and discards", notes: 1, restoreErr: settingFailure},
		{name: "load and restore errors preserved", notes: 1, loadErr: loadFailure, restoreErr: settingFailure},
		{name: "canceled load cleans up", notes: 1, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := &diagnosticState{notes: test.notes, total: int64(0), loadErr: test.loadErr, disableErr: test.disableErr, restoreErr: test.restoreErr, beginErr: test.beginErr}
			if test.cancel {
				state.cancel = cancel
			}
			db := sql.OpenDB(&diagnosticConnector{state: state})
			defer db.Close()
			_, err := NewSession(&info.Dialect{Transactional: true}).Exec(ctx, []struct{ ID int }{{1}}, db, "test_load")
			state.mu.Lock()
			defer state.mu.Unlock()
			wantErr := test.loadErr != nil || test.disableErr != nil || test.restoreErr != nil || test.beginErr != nil || test.cancel
			if (err != nil) != wantErr {
				t.Fatalf("err=%v, want error=%v", err, wantErr)
			}
			for _, expected := range []error{test.loadErr, test.disableErr, test.restoreErr, test.beginErr} {
				if expected != nil && !errors.Is(err, expected) {
					t.Fatalf("lost error %v: %v", expected, err)
				}
			}
			if test.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if wantErr && state.commits != 0 {
				t.Fatal("committed failed load")
			}
			if !wantErr && (state.commits != 1 || state.endNotes != test.notes) {
				t.Fatalf("commit=%d sql_notes at commit=%d", state.commits, state.endNotes)
			}
			if test.restoreErr != nil && state.closed == 0 {
				t.Fatal("connection with failed restoration was pooled")
			}
			if state.notes != test.notes && state.closed == 0 {
				t.Fatal("connection pooled with modified notes")
			}
			if test.notes == 0 {
				for _, query := range state.queries {
					if strings.HasPrefix(query, "SET SESSION") {
						t.Fatalf("unnecessary setting change: %s", query)
					}
				}
			}
		})
	}
}

type diagnosticState struct {
	mu                                        sync.Mutex
	total                                     driver.Value
	diagnostics                               [][]driver.Value
	countErr, queryErr, iterateErr            error
	commits, rollbacks, connections, openRows int
	queries                                   []string
	notes                                     int
	loadErr, disableErr, restoreErr, beginErr error
	cancel                                    context.CancelFunc
	closed                                    int
	endNotes                                  int
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
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.connections++
	return &diagnosticConn{state: c.state}, nil
}
func (*diagnosticConnector) Driver() driver.Driver        { return diagnosticDriver{} }
func (diagnosticDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (*diagnosticConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *diagnosticConn) Close() error {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.closed++
	return nil
}
func (c *diagnosticConn) Begin() (driver.Tx, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.beginErr != nil {
		return nil, c.state.beginErr
	}
	return &diagnosticTx{state: c.state}, nil
}
func (t *diagnosticTx) Commit() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.commits++
	t.state.endNotes = t.state.notes
	return nil
}
func (t *diagnosticTx) Rollback() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.rollbacks++
	return nil
}
func (c *diagnosticConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.HasPrefix(query, "SET SESSION sql_notes=") {
		c.state.queries = append(c.state.queries, query)
		if query == "SET SESSION sql_notes=0" {
			if c.state.disableErr != nil {
				return nil, c.state.disableErr
			}
			c.state.notes = 0
		} else {
			if c.state.restoreErr != nil {
				return nil, c.state.restoreErr
			}
			c.state.notes = 1
		}
		return driver.RowsAffected(0), nil
	}
	if !strings.HasPrefix(query, "LOAD DATA") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	c.state.queries = append(c.state.queries, "LOAD")
	if c.state.cancel != nil {
		c.state.cancel()
		return nil, context.Canceled
	}
	if c.state.loadErr != nil {
		return nil, c.state.loadErr
	}
	return driver.RowsAffected(1), nil
}
func (c *diagnosticConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.queries = append(c.state.queries, query)
	rows := &diagnosticRows{state: c.state}
	switch query {
	case "SELECT @@session.sql_notes":
		rows.columns = []string{"@@session.sql_notes"}
		rows.values = [][]driver.Value{{int64(c.state.notes)}}
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
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
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
