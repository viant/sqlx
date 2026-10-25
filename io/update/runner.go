package update

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/sqlx/option"
)

var (
	ErrRunnerBusy        = errors.New("update statement runner is busy")
	ErrRunnerClosed      = errors.New("update statement runner is closed")
	ErrRunnerTransaction = errors.New("update statement runner requires its caller-owned transaction")
)

// RunnerPanicError preserves a panic together with failures encountered while
// joining execution or cleaning up. The original panic remains in Value.
type RunnerPanicError struct {
	Value any
	Cause error
}

func (e *RunnerPanicError) Error() string { return "update statement runner panicked" }
func (e *RunnerPanicError) Unwrap() []error {
	var result []error
	if e.Cause != nil {
		result = append(result, e.Cause)
	}
	if cause, ok := e.Value.(error); ok {
		result = append(result, cause)
	}
	return result
}

// StatementRunner reuses at most one exact UPDATE shape within one callback.
// It must not be copied. It never owns transaction completion.
type StatementRunner struct {
	mu             sync.Mutex
	idle           *sync.Cond
	active, closed bool
	failure        error
	tx             *sql.Tx
	key            runnerKey
	stmt           *sql.Stmt
}

type runnerKey struct {
	service *Service
	mapping *session
	rowType reflect.Type
	builder *Builder
	marker  *option.SetMarker
	query   string
}

// WithStatementRunner seals admission, joins accepted execution and closes the
// retained statement before returning. Callback/execution/cleanup errors are
// retained even if the callback ignores an execution error. Panics propagate
// after cleanup. The caller alone must commit or roll back tx.
func WithStatementRunner(tx *sql.Tx, callback func(*StatementRunner) error) (err error) {
	if tx == nil {
		return ErrRunnerTransaction
	}
	if callback == nil {
		return fmt.Errorf("update statement runner callback is required")
	}
	r := &StatementRunner{tx: tx}
	r.idle = sync.NewCond(&r.mu)
	defer func() {
		value := recover()
		failure, cleanup, cleanupPanic := r.finish()
		err = joinRunnerErrors(joinRunnerErrors(err, failure), cleanup)
		if value != nil {
			if cleanup != nil || independentRunnerFailure(failure, value) {
				panic(&RunnerPanicError{Value: value, Cause: err})
			}
			panic(value)
		}
		if cleanupPanic != nil {
			panic(&RunnerPanicError{Value: cleanupPanic, Cause: err})
		}
	}()
	return callback(r)
}

// Exec preserves ordinary per-row binding, hooks, cancellation and guards.
// Concurrent/reentrant execution is rejected. Calls after the callback return
// ErrRunnerClosed.
// Nontransactional dialects and custom builders use ordinary, unretained statements.
func (r *StatementRunner) Exec(ctx context.Context, service *Service, value any, options ...option.Option) (count int64, err error) {
	if err = r.acquire(); err != nil {
		return 0, err
	}
	defer func() { r.release(err, recover()) }()
	if ctx == nil {
		return 0, fmt.Errorf("update execution context is required")
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if service == nil || service.Config == nil || service.Dialect == nil {
		return 0, fmt.Errorf("configured update service is required")
	}
	for _, opt := range options {
		if tx, ok := opt.(*sql.Tx); ok && tx != nil && tx != r.tx {
			return 0, ErrRunnerTransaction
		}
	}
	owned := make([]option.Option, len(options), len(options)+1)
	copy(owned, options)
	owned = append(owned, r.tx)
	if !service.Dialect.Transactional {
		if err = r.reset(); err != nil {
			return 0, err
		}
	}
	return service.exec(ctx, value, r, owned...)
}

// Reset invalidates reuse at a SQL/DDL or queue barrier. It does not complete
// the transaction and is subject to the same serial admission as Exec.
func (r *StatementRunner) Reset() (err error) {
	if err = r.acquire(); err != nil {
		return err
	}
	defer func() { r.release(err, recover()) }()
	return r.reset()
}

func (r *StatementRunner) acquire() error {
	if r == nil || r.idle == nil {
		return ErrRunnerClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRunnerClosed
	}
	if r.active {
		r.failure = joinRunnerErrors(r.failure, ErrRunnerBusy)
		return ErrRunnerBusy
	}
	r.active = true
	return nil
}

func (r *StatementRunner) release(err error, value any) {
	r.mu.Lock()
	if value != nil {
		err = joinRunnerErrors(err, &RunnerPanicError{Value: value})
	}
	r.failure = joinRunnerErrors(r.failure, err)
	r.active = false
	r.idle.Broadcast()
	r.mu.Unlock()
	if value != nil {
		panic(value)
	}
}

func (r *StatementRunner) prepare(ctx context.Context, s *session, builder *Builder, query string) (*sql.Stmt, error) {
	if s.Transaction == nil || s.Transaction.Tx != r.tx {
		return nil, ErrRunnerTransaction
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := runnerKey{service: s.service, mapping: s.mapping, rowType: s.rType, builder: builder, marker: s.setMarker, query: query}
	if r.stmt != nil && r.key == key {
		return r.stmt, nil
	}
	if err := r.reset(); err != nil {
		return nil, err
	}
	if showSQL {
		fmt.Println(query)
	}
	stmt, err := r.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	r.stmt, r.key = stmt, key
	return stmt, nil
}

// The active execution or sealed finalizer alone owns physical cleanup.
func (r *StatementRunner) reset() (err error) {
	stmt := r.stmt
	r.stmt, r.key = nil, runnerKey{}
	defer func() {
		value := recover()
		if err != nil || value != nil {
			r.mu.Lock()
			r.closed = true
			r.mu.Unlock()
		}
		if value != nil {
			panic(value)
		}
	}()
	if stmt != nil {
		return stmt.Close()
	}
	return nil
}

func (r *StatementRunner) finish() (failure, cleanup error, value any) {
	r.mu.Lock()
	r.closed = true
	for r.active {
		r.idle.Wait()
	}
	failure = r.failure
	r.mu.Unlock()
	defer func() {
		value = recover()
		if value != nil {
			cleanup = &RunnerPanicError{Value: value}
		}
	}()
	cleanup = r.reset()
	return
}

func joinRunnerErrors(first, next error) error {
	if first == nil {
		return next
	}
	if next == nil || errors.Is(first, next) {
		return first
	}
	return errors.Join(first, next)
}

// A panic already recorded by Exec need not be wrapped again. Other failures
// must remain discoverable when a callback subsequently panics.
func independentRunnerFailure(failure error, value any) bool {
	if failure == nil {
		return false
	}
	if recorded, ok := failure.(*RunnerPanicError); ok && recorded.Cause == nil {
		return !reflect.TypeOf(value).Comparable() || recorded.Value != value
	}
	return true
}
