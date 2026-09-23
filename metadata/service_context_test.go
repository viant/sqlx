package metadata_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	sqliteproduct "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/option"
)

const blockingMetadataDriverName = "sqlx-blocking-metadata"

var registerBlockingMetadataDriver sync.Once
var blockingMetadataScenarios sync.Map
var blockingMetadataScenarioSeq atomic.Int64

type blockingMetadataScenario struct {
	mode    string
	columns []string
	values  []driver.Value

	prepareCalls      atomic.Int32
	queryCalls        atomic.Int32
	queryContextCalls atomic.Int32
	stmtCloseCalls    atomic.Int32
	rowsCloseCalls    atomic.Int32
	paramCount        atomic.Int32

	startedOnce sync.Once
	started     chan struct{}

	mu       sync.Mutex
	ctxErr   error
	querySQL string
}

type blockingMetadataDriver struct{}

type blockingMetadataConn struct {
	scenario *blockingMetadataScenario
}

type blockingMetadataStmt struct {
	scenario *blockingMetadataScenario
}

type blockingMetadataTx struct{}

type blockingMetadataRows struct {
	scenario *blockingMetadataScenario
	columns  []string
	values   []driver.Value
	served   bool
}

func TestServiceInfo_QueryContextHonorsCancellation(t *testing.T) {
	testCases := []struct {
		name      string
		useTx     bool
		kind      info.Kind
		options   []option.Option
		wantError error
		ctxFn     func(*blockingMetadataScenario) (context.Context, context.CancelFunc)
	}{
		{
			name:      "db_deadline_no_params",
			kind:      info.KindVersion,
			wantError: context.DeadlineExceeded,
			ctxFn: func(_ *blockingMetadataScenario) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
		},
		{
			name:      "db_cancel_with_params",
			kind:      info.KindSchema,
			options:   []option.Option{option.NewArgs("", "main")},
			wantError: context.Canceled,
			ctxFn: func(s *blockingMetadataScenario) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					<-s.started
					cancel()
				}()
				return ctx, cancel
			},
		},
		{
			name:      "tx_deadline_no_params",
			useTx:     true,
			kind:      info.KindVersion,
			wantError: context.DeadlineExceeded,
			ctxFn: func(_ *blockingMetadataScenario) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
		},
		{
			name:      "tx_cancel_with_params",
			useTx:     true,
			kind:      info.KindSchema,
			options:   []option.Option{option.NewArgs("", "main")},
			wantError: context.Canceled,
			ctxFn: func(s *blockingMetadataScenario) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					<-s.started
					cancel()
				}()
				return ctx, cancel
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scenario := &blockingMetadataScenario{
				mode:    "block",
				started: make(chan struct{}),
			}
			db := openBlockingMetadataDB(t, scenario)
			defer db.Close()

			options := append([]option.Option{sqliteproduct.SQLite3()}, testCase.options...)
			if testCase.useTx {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatalf("BeginTx() error = %v", err)
				}
				defer tx.Rollback()
				options = append(options, tx)
			}

			ctx, cancel := testCase.ctxFn(scenario)
			defer cancel()

			meta := metadata.New()
			var actual string
			err := meta.Info(ctx, db, testCase.kind, &actual, options...)
			if !errors.Is(err, testCase.wantError) {
				t.Fatalf("Info() error = %v, want %v", err, testCase.wantError)
			}
			if got := scenario.queryContextCalls.Load(); got != 1 {
				t.Fatalf("QueryContext call count = %d, want 1", got)
			}
			if got := scenario.queryCalls.Load(); got != 0 {
				t.Fatalf("Query call count = %d, want 0", got)
			}
			if got := scenario.prepareCalls.Load(); got != 1 {
				t.Fatalf("PrepareContext call count = %d, want 1", got)
			}
			if got := scenario.stmtCloseCalls.Load(); got != 1 {
				t.Fatalf("Stmt close count = %d, want 1", got)
			}
			if got := scenario.rowsCloseCalls.Load(); got != 0 {
				t.Fatalf("Rows close count = %d, want 0", got)
			}
			if !errors.Is(scenario.contextErr(), testCase.wantError) {
				t.Fatalf("execution context error = %v, want %v", scenario.contextErr(), testCase.wantError)
			}
			if len(testCase.options) > 0 && scenario.paramCount.Load() == 0 {
				t.Fatalf("expected query parameters to be passed")
			}
			if len(testCase.options) == 0 && scenario.paramCount.Load() != 0 {
				t.Fatalf("expected no query parameters, got %d", scenario.paramCount.Load())
			}
		})
	}
}

func TestServiceInfo_QueryContextClosesResourcesOnSuccess(t *testing.T) {
	testCases := []struct {
		name       string
		useTx      bool
		kind       info.Kind
		options    []option.Option
		columns    []string
		values     []driver.Value
		wantValue  string
		wantParams int
	}{
		{
			name:       "db_no_params",
			kind:       info.KindVersion,
			columns:    []string{"version"},
			values:     []driver.Value{"SQLite - 3.45.0"},
			wantValue:  "SQLite - 3.45.0",
			wantParams: 0,
		},
		{
			name:       "tx_with_params",
			useTx:      true,
			kind:       info.KindSchema,
			options:    []option.Option{option.NewArgs("", "main")},
			columns:    []string{"name"},
			values:     []driver.Value{"main"},
			wantValue:  "main",
			wantParams: 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scenario := &blockingMetadataScenario{
				mode:    "success",
				columns: testCase.columns,
				values:  testCase.values,
				started: make(chan struct{}),
			}
			db := openBlockingMetadataDB(t, scenario)
			defer db.Close()

			options := append([]option.Option{sqliteproduct.SQLite3()}, testCase.options...)
			if testCase.useTx {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatalf("BeginTx() error = %v", err)
				}
				defer tx.Rollback()
				options = append(options, tx)
			}

			meta := metadata.New()
			var actual string
			if err := meta.Info(context.Background(), db, testCase.kind, &actual, options...); err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if actual != testCase.wantValue {
				t.Fatalf("Info() value = %q, want %q", actual, testCase.wantValue)
			}
			if got := scenario.queryContextCalls.Load(); got != 1 {
				t.Fatalf("QueryContext call count = %d, want 1", got)
			}
			if got := scenario.queryCalls.Load(); got != 0 {
				t.Fatalf("Query call count = %d, want 0", got)
			}
			if got := scenario.stmtCloseCalls.Load(); got != 1 {
				t.Fatalf("Stmt close count = %d, want 1", got)
			}
			if got := scenario.rowsCloseCalls.Load(); got != 1 {
				t.Fatalf("Rows close count = %d, want 1", got)
			}
			if got := int(scenario.paramCount.Load()); got != testCase.wantParams {
				t.Fatalf("parameter count = %d, want %d", got, testCase.wantParams)
			}
		})
	}
}

func openBlockingMetadataDB(t *testing.T, scenario *blockingMetadataScenario) *sql.DB {
	t.Helper()
	registerBlockingMetadataDriver.Do(func() {
		sql.Register(blockingMetadataDriverName, &blockingMetadataDriver{})
	})
	dsn := fmt.Sprintf("scenario-%d", blockingMetadataScenarioSeq.Add(1))
	blockingMetadataScenarios.Store(dsn, scenario)
	t.Cleanup(func() {
		blockingMetadataScenarios.Delete(dsn)
	})
	db, err := sql.Open(blockingMetadataDriverName, dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	return db
}

func (s *blockingMetadataScenario) contextErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctxErr
}

func (d *blockingMetadataDriver) Open(name string) (driver.Conn, error) {
	value, ok := blockingMetadataScenarios.Load(name)
	if !ok {
		return nil, fmt.Errorf("blocking metadata scenario %q not found", name)
	}
	return &blockingMetadataConn{scenario: value.(*blockingMetadataScenario)}, nil
}

func (c *blockingMetadataConn) Prepare(query string) (driver.Stmt, error) {
	return &blockingMetadataStmt{scenario: c.scenario}, nil
}

func (c *blockingMetadataConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.scenario.prepareCalls.Add(1)
	c.scenario.mu.Lock()
	c.scenario.querySQL = query
	c.scenario.mu.Unlock()
	return &blockingMetadataStmt{scenario: c.scenario}, nil
}

func (c *blockingMetadataConn) Close() error { return nil }

func (c *blockingMetadataConn) Begin() (driver.Tx, error) {
	return &blockingMetadataTx{}, nil
}

func (c *blockingMetadataConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &blockingMetadataTx{}, nil
}

func (s *blockingMetadataStmt) Close() error {
	s.scenario.stmtCloseCalls.Add(1)
	return nil
}

func (s *blockingMetadataStmt) NumInput() int { return -1 }

func (s *blockingMetadataStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("unexpected Exec without context")
}

func (s *blockingMetadataStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.scenario.queryCalls.Add(1)
	return nil, errors.New("unexpected Query without context")
}

func (s *blockingMetadataStmt) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("unexpected ExecContext")
}

func (s *blockingMetadataStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.scenario.queryContextCalls.Add(1)
	s.scenario.paramCount.Store(int32(len(args)))
	s.scenario.startedOnce.Do(func() {
		close(s.scenario.started)
	})
	switch s.scenario.mode {
	case "success":
		return &blockingMetadataRows{
			scenario: s.scenario,
			columns:  append([]string(nil), s.scenario.columns...),
			values:   append([]driver.Value(nil), s.scenario.values...),
		}, nil
	default:
		<-ctx.Done()
		s.scenario.mu.Lock()
		s.scenario.ctxErr = ctx.Err()
		s.scenario.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (t *blockingMetadataTx) Commit() error   { return nil }
func (t *blockingMetadataTx) Rollback() error { return nil }

func (r *blockingMetadataRows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *blockingMetadataRows) Close() error {
	r.scenario.rowsCloseCalls.Add(1)
	return nil
}

func (r *blockingMetadataRows) Next(dest []driver.Value) error {
	if r.served {
		return io.EOF
	}
	r.served = true
	copy(dest, r.values)
	return nil
}

var _ driver.Driver = (*blockingMetadataDriver)(nil)
var _ driver.Conn = (*blockingMetadataConn)(nil)
var _ driver.ConnPrepareContext = (*blockingMetadataConn)(nil)
var _ driver.ConnBeginTx = (*blockingMetadataConn)(nil)
var _ driver.Stmt = (*blockingMetadataStmt)(nil)
var _ driver.StmtQueryContext = (*blockingMetadataStmt)(nil)
var _ driver.StmtExecContext = (*blockingMetadataStmt)(nil)
var _ driver.Tx = (*blockingMetadataTx)(nil)
var _ driver.Rows = (*blockingMetadataRows)(nil)
