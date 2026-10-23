package update

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

type runnerState struct {
	mu                                             sync.Mutex
	prepares, executes, closes, commits, rollbacks int
	queries                                        []string
	args                                           [][]driver.NamedValue
	exec                                           func(context.Context, []driver.NamedValue) (driver.Result, error)
	closeErr                                       error
	closePanic                                     any
}
type runnerConnector struct{ state *runnerState }

func (c runnerConnector) Connect(context.Context) (driver.Conn, error) {
	return &runnerConn{c.state}, nil
}
func (c runnerConnector) Driver() driver.Driver            { return c }
func (c runnerConnector) Open(string) (driver.Conn, error) { return c.Connect(context.Background()) }

type runnerConn struct{ state *runnerState }

func (c *runnerConn) Close() error              { return nil }
func (c *runnerConn) Begin() (driver.Tx, error) { return &runnerTx{c.state}, nil }
func (c *runnerConn) Prepare(q string) (driver.Stmt, error) {
	c.state.mu.Lock()
	c.state.prepares++
	c.state.queries = append(c.state.queries, q)
	c.state.mu.Unlock()
	return &runnerStmt{c.state}, nil
}

type runnerTx struct{ state *runnerState }

func (t *runnerTx) Commit() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.commits++
	return nil
}
func (t *runnerTx) Rollback() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.rollbacks++
	return nil
}

type runnerStmt struct{ state *runnerState }

func (s *runnerStmt) NumInput() int                             { return -1 }
func (s *runnerStmt) Query([]driver.Value) (driver.Rows, error) { return nil, io.EOF }
func (s *runnerStmt) Exec(values []driver.Value) (driver.Result, error) {
	args := make([]driver.NamedValue, len(values))
	for i, v := range values {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), args)
}
func (s *runnerStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.state.mu.Lock()
	s.state.executes++
	s.state.args = append(s.state.args, append([]driver.NamedValue(nil), args...))
	fn := s.state.exec
	s.state.mu.Unlock()
	if fn != nil {
		return fn(ctx, args)
	}
	return driver.RowsAffected(1), nil
}
func (s *runnerStmt) Close() error {
	s.state.mu.Lock()
	s.state.closes++
	err, value := s.state.closeErr, s.state.closePanic
	s.state.mu.Unlock()
	if value != nil {
		panic(value)
	}
	return err
}

type runnerHas struct{ Name, Version, ID bool }
type runnerRow struct {
	Name    string                      `sqlx:"name"`
	Version int                         `sqlx:"version"`
	ID      int                         `sqlx:"id,primaryKey"`
	Has     *runnerHas                  `sqlx:"-" setMarker:"true"`
	hook    func(context.Context) error `sqlx:"-"`
}

func (r *runnerRow) OnUpdate(ctx context.Context) error {
	if r.hook != nil {
		return r.hook(ctx)
	}
	return nil
}
func runnerFixture(t *testing.T) (*Service, *sql.Tx, *runnerState) {
	t.Helper()
	state := &runnerState{}
	db := sql.OpenDB(runnerConnector{state})
	t.Cleanup(func() { db.Close() })
	svc, err := New(context.Background(), db, "records", &info.Dialect{Transactional: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return svc, tx, state
}
func testRunnerRow(id int) *runnerRow {
	return &runnerRow{ID: id, Name: fmt.Sprint(id), Has: &runnerHas{Name: true, ID: true}}
}
func runnerCounts(state *runnerState) (int, int, int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.prepares, state.executes, state.closes
}

func TestStatementRunnerReuseFreshBindingsAndReset(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	ctx := context.Background()
	var escaped *StatementRunner
	hooks := 0
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		escaped = r
		for i := 1; i <= 3; i++ {
			row := testRunnerRow(i)
			row.hook = func(context.Context) error { hooks++; return nil }
			n, err := r.Exec(ctx, svc, row, option.IfMatch{Column: "version", Value: i}, &sqlx.Criteria{Expression: "name <> ?", Placeholders: []any{fmt.Sprint(-i)}})
			if err != nil || n != 1 {
				t.Fatalf("count=%d err=%v", n, err)
			}
		}
		if p, e, c := runnerCounts(state); p != 1 || e != 3 || c != 0 {
			t.Fatalf("counts before reset %d %d %d", p, e, c)
		}
		if err := r.Reset(); err != nil {
			return err
		}
		_, err := r.Exec(ctx, svc, testRunnerRow(4))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if p, e, c := runnerCounts(state); p != 2 || e != 4 || c != 2 {
		t.Fatalf("counts %d %d %d", p, e, c)
	}
	if hooks != 3 {
		t.Fatal("hook count", hooks)
	}
	for i := 1; i <= 3; i++ {
		want := []driver.NamedValue{{Ordinal: 1, Value: fmt.Sprint(i)}, {Ordinal: 2, Value: int64(i)}, {Ordinal: 3, Value: int64(i)}, {Ordinal: 4, Value: fmt.Sprint(-i)}}
		if !reflect.DeepEqual(state.args[i-1], want) {
			t.Fatalf("row %d bindings=%v", i, state.args[i-1])
		}
	}
	if state.commits != 0 || state.rollbacks != 0 {
		t.Fatal("runner completed caller transaction")
	}
	if _, err := escaped.Exec(ctx, svc, testRunnerRow(5)); !errors.Is(err, ErrRunnerClosed) {
		t.Fatal(err)
	}
	if err := escaped.Reset(); !errors.Is(err, ErrRunnerClosed) {
		t.Fatal(err)
	}
}

type runnerAlternate struct {
	ID   int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}
type runnerCustomBuilder struct{}

func (runnerCustomBuilder) Build(interface{}, ...option.Option) string {
	return "UPDATE records SET name = ? WHERE id = ?"
}

func TestStatementRunnerMappingShapesAndCustomFallback(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		ctx := context.Background()
		for _, row := range []any{testRunnerRow(1), &runnerRow{ID: 1, Version: 2, Has: &runnerHas{Version: true, ID: true}}, &runnerAlternate{ID: 1, Name: "other"}, testRunnerRow(1)} {
			if _, err := r.Exec(ctx, svc, row); err != nil {
				return err
			}
		}
		if p, _, c := runnerCounts(state); p != 4 || c != 3 {
			t.Fatalf("shape/mapping counts %d %d", p, c)
		}
		svc.Config.Builder = runnerCustomBuilder{}
		for i := 0; i < 2; i++ {
			if _, err := r.Exec(ctx, svc, testRunnerRow(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if p, e, c := runnerCounts(state); p != 6 || e != 6 || c != 6 {
		t.Fatalf("custom fallback counts %d %d %d", p, e, c)
	}
}

func TestStatementRunnerTransactionAndNontransactionalFallback(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	_, otherTx, _ := runnerFixture(t)
	if err := WithStatementRunner(nil, func(*StatementRunner) error { t.Fatal("nil tx callback"); return nil }); !errors.Is(err, ErrRunnerTransaction) {
		t.Fatal(err)
	}
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		_, err := r.Exec(context.Background(), svc, testRunnerRow(1), otherTx)
		if !errors.Is(err, ErrRunnerTransaction) {
			t.Fatal(err)
		}
		return nil
	})
	if !errors.Is(err, ErrRunnerTransaction) {
		t.Fatal("ignored transaction mismatch lost", err)
	}
	if p, e, _ := runnerCounts(state); p != 0 || e != 0 {
		t.Fatal("foreign tx reached driver")
	}
	svc.Dialect = &info.Dialect{Transactional: false}
	err = WithStatementRunner(tx, func(r *StatementRunner) error {
		for i := 0; i < 2; i++ {
			if _, err := r.Exec(context.Background(), svc, testRunnerRow(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if p, e, c := runnerCounts(state); p != 2 || e != 2 || c != 2 {
		t.Fatalf("fallback counts %d %d %d", p, e, c)
	}
	if state.commits != 0 || state.rollbacks != 0 {
		t.Fatal("completed caller transaction")
	}
}

type runnerCause struct{ message string }

func (e *runnerCause) Error() string { return e.message }
func TestStatementRunnerRetainsErrorsAndCleanup(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	execution := &runnerCause{"execution"}
	callback := errors.New("callback")
	cleanup := errors.New("cleanup")
	state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) { return nil, execution }
	state.closeErr = cleanup
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		_, _ = r.Exec(context.Background(), svc, testRunnerRow(1))
		return callback
	})
	for _, cause := range []error{execution, callback, cleanup} {
		if !errors.Is(err, cause) {
			t.Fatalf("lost %v: %v", cause, err)
		}
	}
	var actual *runnerCause
	if !errors.As(err, &actual) || actual != execution {
		t.Fatal("typed cause lost", err)
	}
	if _, _, c := runnerCounts(state); c != 1 {
		t.Fatal("cleanup repeated", c)
	}
	if state.commits != 0 || state.rollbacks != 0 {
		t.Fatal("runner completed caller transaction")
	}
}

func TestStatementRunnerReentrantHookFailsPromptly(t *testing.T) {
	svc, tx, _ := runnerFixture(t)
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		row := testRunnerRow(1)
		row.hook = func(ctx context.Context) error {
			if _, err := r.Exec(ctx, svc, testRunnerRow(2)); !errors.Is(err, ErrRunnerBusy) {
				t.Fatal(err)
			}
			if err := r.Reset(); !errors.Is(err, ErrRunnerBusy) {
				t.Fatal(err)
			}
			return nil
		}
		_, err := r.Exec(context.Background(), svc, row)
		return err
	})
	if !errors.Is(err, ErrRunnerBusy) {
		t.Fatal("ignored reentrant failure lost", err)
	}
}

func waitRunnerSealed(t *testing.T, r *StatementRunner) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("finalizer did not seal")
		}
		runtime.Gosched()
	}
}

func TestStatementRunnerJoinsEscapedExecution(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	published := make(chan *StatementRunner, 1)
	finished := make(chan error, 1)
	child := make(chan error, 1)
	cause := errors.New("escaped execution")
	state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) {
		close(started)
		<-release
		return nil, cause
	}
	go func() {
		finished <- WithStatementRunner(tx, func(r *StatementRunner) error {
			published <- r
			go func() { _, err := r.Exec(context.Background(), svc, testRunnerRow(1)); child <- err }()
			<-started
			return nil
		})
	}()
	r := <-published
	waitRunnerSealed(t, r)
	if _, err := r.Exec(context.Background(), svc, testRunnerRow(2)); !errors.Is(err, ErrRunnerClosed) {
		t.Fatal(err)
	}
	if _, _, c := runnerCounts(state); c != 0 {
		t.Fatal("closed underneath active execution")
	}
	select {
	case err := <-finished:
		t.Fatal("returned before joining", err)
	default:
	}
	close(release)
	if err := <-child; !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, cause) {
		t.Fatal("escaped error lost", err)
	}
	if _, _, c := runnerCounts(state); c != 1 {
		t.Fatal("close count", c)
	}
}

func TestStatementRunnerConcurrentAdmission(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) {
		close(started)
		<-release
		return driver.RowsAffected(1), nil
	}
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		go func() { _, err := r.Exec(context.Background(), svc, testRunnerRow(1)); done <- err }()
		<-started
		if _, err := r.Exec(context.Background(), svc, testRunnerRow(2)); !errors.Is(err, ErrRunnerBusy) {
			t.Fatal(err)
		}
		close(release)
		return <-done
	})
	if !errors.Is(err, ErrRunnerBusy) {
		t.Fatal("concurrent error lost", err)
	}
}

func recoverRunner(fn func()) (value any) { defer func() { value = recover() }(); fn(); return nil }

func TestStatementRunnerPanicUnwinding(t *testing.T) {
	for _, phase := range []string{"callback", "hook", "fallback-hook", "driver", "cleanup", "callback-and-cleanup"} {
		t.Run(phase, func(t *testing.T) {
			// database/sql does not defer the transaction lease release around a
			// driver panic. Isolate that case: even Rollback would block afterward.
			if phase == "driver" && os.Getenv("SQLX_RUNNER_DRIVER_PANIC") != "1" {
				cmd := exec.Command(os.Args[0], "-test.run=^TestStatementRunnerPanicUnwinding/driver$", "-test.timeout=10s")
				cmd.Env = append(os.Environ(), "SQLX_RUNNER_DRIVER_PANIC=1")
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("driver panic subprocess: %v\n%s", err, output)
				}
				return
			}
			svc, tx, state := runnerFixture(t)
			if phase == "fallback-hook" {
				svc.Dialect = &info.Dialect{Transactional: false}
			}
			original := errors.New("panic " + phase)
			cleanup := errors.New("close")
			var escaped *StatementRunner
			if phase == "driver" {
				state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) { panic(original) }
			}
			if phase == "cleanup" {
				state.closePanic = original
			}
			if phase == "callback-and-cleanup" {
				state.closeErr = cleanup
			}
			value := recoverRunner(func() {
				_ = WithStatementRunner(tx, func(r *StatementRunner) error {
					escaped = r
					row := testRunnerRow(1)
					if phase == "hook" || phase == "fallback-hook" {
						row.hook = func(context.Context) error { panic(original) }
					}
					if _, err := r.Exec(context.Background(), svc, row); err != nil {
						return err
					}
					if phase == "callback" || phase == "callback-and-cleanup" {
						panic(original)
					}
					return nil
				})
			})
			if phase == "cleanup" || phase == "callback-and-cleanup" {
				p, ok := value.(*RunnerPanicError)
				if !ok || p.Value != original {
					t.Fatalf("panic changed: %#v", value)
				}
				if phase == "callback-and-cleanup" && !errors.Is(p, cleanup) {
					t.Fatal("cleanup cause lost")
				}
			} else if value != original {
				t.Fatalf("panic changed: %#v", value)
			}
			if _, _, c := runnerCounts(state); c != 1 {
				t.Fatal("cleanup not exactly once", c)
			}
			if _, err := escaped.Exec(context.Background(), svc, testRunnerRow(2)); !errors.Is(err, ErrRunnerClosed) {
				t.Fatal(err)
			}
			if state.commits != 0 || state.rollbacks != 0 {
				t.Fatal("runner completed tx")
			}
			if phase == "driver" {
				os.Exit(0)
			}
		})
	}
}

func TestStatementRunnerCanceledReuse(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		if _, err := r.Exec(ctx, svc, testRunnerRow(1)); err != nil {
			return err
		}
		cancel()
		_, err := r.Exec(ctx, svc, testRunnerRow(2))
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if p, e, c := runnerCounts(state); p != 1 || e != 1 || c != 1 {
		t.Fatalf("canceled counts %d %d %d", p, e, c)
	}
}

func TestStatementRunnerSQLiteGuardsAndRollback(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "runner.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT, version INTEGER); INSERT INTO records VALUES (1, 'old', 1), (2, 'old', 2)"); err != nil {
		t.Fatal(err)
	}
	svc, err := New(ctx, db, "records", &info.Dialect{Transactional: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = WithStatementRunner(tx, func(r *StatementRunner) error {
		for id := 1; id <= 2; id++ {
			row := testRunnerRow(id)
			row.Name = fmt.Sprintf("new%d", id)
			if n, err := r.Exec(ctx, svc, row, option.IfMatch{Column: "version", Value: id}); err != nil || n != 1 {
				t.Fatalf("update %d: %d %v", id, n, err)
			}
		}
		var name string
		if err := tx.QueryRowContext(ctx, "SELECT name FROM records WHERE id = 1").Scan(&name); err != nil || name != "new1" {
			t.Fatalf("transaction visibility: %q %v", name, err)
		}
		// An ignored stale guard must still prevent the owner from committing.
		_, stale := r.Exec(ctx, svc, testRunnerRow(2), option.IfMatch{Column: "version", Value: 99})
		if !errors.Is(stale, option.ErrNoMatch) {
			t.Fatal(stale)
		}
		return nil
	})
	if !errors.Is(err, option.ErrNoMatch) {
		t.Fatal("lost guard error", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM records WHERE name = 'old'").Scan(&count); err != nil || count != 2 {
		t.Fatalf("rollback: %d %v", count, err)
	}
	// A new callback uses fresh evidence and leaves completion to the owner.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = WithStatementRunner(tx, func(r *StatementRunner) error {
		_, err := r.Exec(ctx, svc, testRunnerRow(1), &sqlx.Criteria{Expression: "version = ?", Placeholders: []any{1}})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRow("SELECT name FROM records WHERE id = 1").Scan(&name); err != nil || name != "1" {
		t.Fatalf("commit: %q %v", name, err)
	}
}

func TestStatementRunnerIndependentConcurrentCallbacks(t *testing.T) {
	svc, _, state := runnerFixture(t)
	// One shared service, distinct transactions and row mappings. The runner's
	// detached builder/session must survive another invocation changing type.
	type otherRow struct {
		Name string `sqlx:"name"`
		ID   int    `sqlx:"id,primaryKey"`
	}
	var wg sync.WaitGroup
	errorsOut := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			tx, err := svc.db.Begin()
			if err != nil {
				errorsOut <- err
				return
			}
			defer tx.Rollback()
			errorsOut <- WithStatementRunner(tx, func(r *StatementRunner) error {
				for i := 0; i < 20; i++ {
					var row any = testRunnerRow(i)
					if worker%2 == 1 {
						row = &otherRow{ID: i, Name: fmt.Sprint(i)}
					}
					if _, err := r.Exec(context.Background(), svc, row); err != nil {
						return err
					}
				}
				return nil
			})
		}(worker)
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, executed, _ := runnerCounts(state); executed != 160 {
		t.Fatal(executed)
	}
}

func TestStatementRunnerInFlightCancellation(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	state.exec = func(ctx context.Context, _ []driver.NamedValue) (driver.Result, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		done <- WithStatementRunner(tx, func(r *StatementRunner) error {
			_, err := r.Exec(ctx, svc, testRunnerRow(1))
			return err
		})
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, closed := runnerCounts(state); closed != 1 {
		t.Fatal(closed)
	}
}

func TestStatementRunnerPresenceMappingInvalidatesReuse(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	err := WithStatementRunner(tx, func(r *StatementRunner) error {
		for i := 0; i < 2; i++ {
			if _, err := r.Exec(context.Background(), svc, testRunnerRow(i), &option.SetMarker{}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if p, e, c := runnerCounts(state); p != 2 || e != 2 || c != 2 {
		t.Fatalf("mapping change reused statement: %d %d %d", p, e, c)
	}
}

func TestStatementRunnerFallbackCleanupPanicPreservesExecutionError(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	svc.Dialect = &info.Dialect{Transactional: false}
	execution, cleanup := errors.New("execute"), errors.New("close panic")
	state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) { return nil, execution }
	state.closePanic = cleanup
	value := recoverRunner(func() {
		_ = WithStatementRunner(tx, func(r *StatementRunner) error {
			_, err := r.Exec(context.Background(), svc, testRunnerRow(1))
			return err
		})
	})
	failure, ok := value.(error)
	if !ok || !errors.Is(failure, execution) || !errors.Is(failure, cleanup) {
		t.Fatalf("lost failure: %#v", value)
	}
	if _, _, closed := runnerCounts(state); closed != 1 {
		t.Fatal(closed)
	}
}

func TestStatementRunnerCallbackPanicPreservesExecutionError(t *testing.T) {
	svc, tx, state := runnerFixture(t)
	execution, callback := errors.New("execution"), errors.New("callback panic")
	state.exec = func(context.Context, []driver.NamedValue) (driver.Result, error) { return nil, execution }
	value := recoverRunner(func() {
		_ = WithStatementRunner(tx, func(r *StatementRunner) error {
			_, _ = r.Exec(context.Background(), svc, testRunnerRow(1))
			panic(callback)
		})
	})
	failure, ok := value.(*RunnerPanicError)
	if !ok || failure.Value != callback || !errors.Is(failure, execution) || !errors.Is(failure, callback) {
		t.Fatalf("lost failure: %#v", value)
	}
	if _, _, closed := runnerCounts(state); closed != 1 {
		t.Fatal(closed)
	}
}

func TestStatementRunnerFailedResetSealsRunner(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprint(panics), func(t *testing.T) {
			svc, tx, state := runnerFixture(t)
			failure := errors.New("close failure")
			var runner *StatementRunner
			err := WithStatementRunner(tx, func(r *StatementRunner) error {
				runner = r
				if _, err := r.Exec(context.Background(), svc, testRunnerRow(1)); err != nil {
					return err
				}
				if panics {
					state.closePanic = failure
				} else {
					state.closeErr = failure
				}
				if value := recoverRunner(func() { _ = r.Reset() }); panics && value != failure {
					t.Fatalf("reset panic: %#v", value)
				}
				if _, err := r.Exec(context.Background(), svc, testRunnerRow(2)); !errors.Is(err, ErrRunnerClosed) {
					t.Fatal(err)
				}
				return nil
			})
			if !errors.Is(err, failure) {
				t.Fatal("ignored reset failure lost", err)
			}
			if err := runner.Reset(); !errors.Is(err, ErrRunnerClosed) {
				t.Fatal(err)
			}
			if _, _, closed := runnerCounts(state); closed != 1 {
				t.Fatal(closed)
			}
		})
	}
}
