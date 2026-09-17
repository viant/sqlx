package read

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/viant/sqlx/io/read/cache"
)

// ErrRetryUnsafe marks a recoverable error that cannot be replayed without
// repeating row callbacks or replacing a caller-owned statement/transaction.
var ErrRetryUnsafe = errors.New("read retry is unsafe after row materialization or with a supplied statement")

// RetryPolicy applies only to QueryAll's database reads. Attempts includes the
// initial attempt. Reconnect resolves the same configured source; it must not
// close the previous pool or replace a caller's transaction.
type RetryPolicy struct {
	Attempts    int
	Recoverable func(error) bool
	Reconnect   func(context.Context) (*sql.DB, error)
}

func WithRetry(policy RetryPolicy) Option {
	return func(o *options) { o.retry = policy }
}

// QueryAll retains one budget across prepare, query-start and pre-row failures.
// There is deliberately no collector reset: even allocation/schema callbacks
// can have effects, so entering row materialization ends replay eligibility.
func (r *Reader) QueryAll(ctx context.Context, emit func(interface{}) error, args ...interface{}) error {
	for attempt := 1; ; attempt++ {
		r.materialized, r.sourceFailed = false, false
		err := r.queryAll(ctx, emit, args...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			r.retry.Attempts <= 1 || r.retry.Recoverable == nil || !r.retry.Recoverable(err) {
			return err
		}
		if r.materialized || r.suppliedStmt {
			return errors.Join(ErrRetryUnsafe, err)
		}
		if !r.sourceFailed || attempt >= r.retry.Attempts || r.retry.Reconnect == nil {
			return err
		}
		if r.stmt != nil {
			_ = r.stmt.Close()
			r.stmt = nil
		}
		db, connectErr := r.retry.Reconnect(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if connectErr != nil {
			return fmt.Errorf("reconnect read source: %w", connectErr)
		}
		if db == nil {
			return fmt.Errorf("reconnect read source returned nil database")
		}
		r.db = db
		// No row or schema callbacks have run; source, mapper and window state
		// are attempt-local inside queryAll. Keep final-attempt cache evidence.
		if r.cacheStats != nil {
			*r.cacheStats = cache.Stats{}
		}
	}
}
