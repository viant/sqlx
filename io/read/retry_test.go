package read_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type retryCacheFailure struct{ cache.Cache }

func (c retryCacheFailure) Get(context.Context, string, []interface{}, ...interface{}) (*cache.Entry, error) {
	return nil, invalidReadConnection
}

var invalidReadConnection = errors.New("driver: invalid connection")

func retryPolicy(db *sql.DB, calls *int) read.RetryPolicy {
	return read.RetryPolicy{Attempts: 3, Recoverable: func(err error) bool {
		return strings.Contains(err.Error(), "invalid connection")
	}, Reconnect: func(context.Context) (*sql.DB, error) { *calls++; return db, nil }}
}

func TestReadRetryDriverSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		phases                                 []string
		row                                    int
		cause                                  error
		cancel, hook, observer                 bool
		prepares, queries, reconnects, emitted int
		unsafe                                 bool
	}{
		{name: "prepare", phases: []string{"prepare"}, prepares: 2, queries: 1, reconnects: 1, emitted: 2},
		{name: "query start", phases: []string{"query"}, prepares: 2, queries: 2, reconnects: 1, emitted: 2},
		{name: "shared budget", phases: []string{"prepare", "query"}, prepares: 3, queries: 2, reconnects: 2, emitted: 2},
		{name: "exhaust prepare", phases: []string{"prepare", "prepare", "prepare"}, prepares: 3, queries: 0, reconnects: 2},
		{name: "exhaust mixed", phases: []string{"query", "prepare", "query"}, prepares: 3, queries: 2, reconnects: 2},
		{name: "before first row", phases: []string{"next"}, prepares: 2, queries: 2, reconnects: 1, emitted: 2},
		{name: "partial rows", phases: []string{"next"}, row: 1, prepares: 1, queries: 1, emitted: 1, unsafe: true},
		{name: "nonrecoverable", phases: []string{"query"}, cause: errors.New("permission denied"), prepares: 1, queries: 1},
		{name: "cancel", phases: []string{"query"}, cancel: true, prepares: 1, queries: 1},
		{name: "hook", hook: true, prepares: 1, queries: 1, emitted: 1, unsafe: true},
		{name: "schema observer", observer: true, prepares: 1, queries: 1, unsafe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prepares, queries, fault, reconnects, emitted, allocated, observed := 0, 0, 0, 0, 0, 0, 0
			cause := tc.cause
			if cause == nil {
				cause = invalidReadConnection
			}
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "prepare" {
					prepares++
				}
				if call.Phase == "query" {
					queries++
				}
				if fault < len(tc.phases) && call.Phase == tc.phases[fault] && call.Row == tc.row {
					fault++
					if tc.cancel {
						cancel()
					}
					return cause
				}
				return nil
			})
			native, err := afs.NewCache(t.TempDir(), time.Minute, "retry", nil)
			require.NoError(t, err)
			query := "SELECT id FROM records ORDER BY id"
			reader, err := read.New(ctx, db, query, func() any { allocated++; return &lookupRow{} },
				read.WithRetry(retryPolicy(db, &reconnects)), read.WithCache(native),
				read.WithColumnsObserver(func(columns []sqlxio.Column) error {
					observed++
					require.Equal(t, "id", columns[0].Name())
					if tc.observer {
						return cause
					}
					return nil
				}))
			require.NoError(t, err)
			err = reader.QueryAll(ctx, func(value any) error {
				emitted++
				require.Equal(t, emitted, value.(*lookupRow).ID)
				if tc.hook {
					return cause
				}
				return nil
			})
			if reader.Stmt() != nil {
				require.NoError(t, reader.Stmt().Close())
			}
			success := tc.emitted == 2
			if success {
				require.NoError(t, err)
			} else if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, tc.unsafe, errors.Is(err, read.ErrRetryUnsafe))
			require.Equal(t, tc.prepares, prepares)
			require.Equal(t, tc.queries, queries)
			require.Equal(t, tc.reconnects, reconnects)
			require.Equal(t, tc.emitted, emitted)
			if tc.emitted > 0 || tc.observer {
				require.Equal(t, 1, observed)
			} else {
				require.Zero(t, allocated)
				require.Zero(t, observed)
			}
			// Failed attempts must release the writer lease without publishing
			// partial rows. A real writer can then populate the same identity.
			if !success {
				entry, lookupErr := native.Lookup(context.Background(), query, nil)
				require.Nil(t, entry)
				require.NoError(t, lookupErr)
				writer, err := read.New(context.Background(), h.DB, query, func() any { return &lookupRow{} }, read.WithCache(native))
				require.NoError(t, err)
				require.NoError(t, writer.QueryAll(context.Background(), func(any) error { return nil }))
				require.NoError(t, writer.Stmt().Close())
			}
			h.Exec(t, "DROP TABLE records")
			hit, err := read.New(context.Background(), db, query, func() any { return &lookupRow{} }, read.WithCache(native), read.WithRetry(retryPolicy(db, &reconnects)))
			require.NoError(t, err)
			values := []int{}
			require.NoError(t, hit.QueryAll(context.Background(), func(row any) error { values = append(values, row.(*lookupRow).ID); return nil }))
			require.Equal(t, []int{1, 2}, values)
			require.Nil(t, hit.Stmt())
			require.Equal(t, tc.reconnects, reconnects)
		})
	}
}

func TestReadRetryScopeCancellationAndReconnectSQLite(t *testing.T) {
	for _, mode := range []string{"scope SQL", "scope args", "canceled before", "deadline", "reconnect error", "reconnect nil", "reconnect cancellation", "fresh source"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(8)")
			calls, reconnects, emitted, observed := 0, 0, 0, 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			db := h.FaultDB(t, func(context.Context, sqlfault.Call) error { calls++; return invalidReadConnection })
			policy := retryPolicy(db, &reconnects)
			reconnectErr := errors.New("source authority unavailable")
			policy.Reconnect = func(context.Context) (*sql.DB, error) {
				reconnects++
				switch mode {
				case "reconnect error":
					return nil, reconnectErr
				case "reconnect nil":
					return nil, nil
				case "reconnect cancellation":
					cancel()
					return h.DB, nil
				}
				return h.DB, nil
			}
			query, args := "SELECT id FROM records WHERE id = ?", []any{8}
			scope, err := read.NewQueryScope([]cache.ParmetrizedQuery{{SQL: query, Args: args}})
			require.NoError(t, err)
			switch mode {
			case "scope SQL":
				query = "select id FROM records WHERE id = ?"
			case "scope args":
				args = []any{9}
			case "canceled before":
				cancel()
			case "deadline":
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer stop()
			}
			reader, err := read.New(ctx, db, query, func() any { return &lookupRow{} }, read.WithRetry(policy), read.WithQueryScope(scope), read.WithColumnsObserver(func(columns []sqlxio.Column) error { observed++; require.Equal(t, "id", columns[0].Name()); return nil }))
			require.NoError(t, err)
			err = reader.QueryAll(ctx, func(value any) error { emitted++; require.Equal(t, 8, value.(*lookupRow).ID); return nil }, args...)
			if reader.Stmt() != nil {
				require.NoError(t, reader.Stmt().Close())
			}
			switch mode {
			case "scope SQL", "scope args":
				require.ErrorIs(t, err, read.ErrQueryOutsideScope)
				require.Zero(t, calls)
			case "canceled before":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, calls)
			case "deadline":
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Zero(t, calls)
			case "reconnect error":
				require.ErrorIs(t, err, reconnectErr)
			case "reconnect nil":
				require.ErrorContains(t, err, "nil database")
			case "reconnect cancellation":
				require.ErrorIs(t, err, context.Canceled)
			case "fresh source":
				require.NoError(t, err)
				require.Equal(t, 1, emitted)
				require.Equal(t, 1, observed)
			}
			if calls > 0 {
				require.Equal(t, 1, calls)
				require.Equal(t, 1, reconnects)
			} else {
				require.Zero(t, reconnects)
			}
		})
	}
}

func TestReadRetryPreservesSuppliedTransactionSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER)")
	queries, reconnects := 0, 0
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" {
			queries++
			if queries == 1 {
				return invalidReadConnection
			}
		}
		return nil
	})
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(9)")
	require.NoError(t, err)
	stmt, err := tx.PrepareContext(ctx, "SELECT id FROM records")
	require.NoError(t, err)
	defer stmt.Close()
	reader := read.NewStmt(stmt, func() any { return &lookupRow{} }, read.WithRetry(retryPolicy(h.DB, &reconnects)))
	require.ErrorIs(t, reader.QueryAll(ctx, func(any) error { t.Fatal("unexpected emit"); return nil }), read.ErrRetryUnsafe)
	require.Zero(t, reconnects)
	var id int
	require.NoError(t, stmt.QueryRowContext(ctx).Scan(&id))
	require.Equal(t, 9, id, "statement must remain in caller transaction")
	require.NoError(t, tx.Rollback())
	var count int
	require.NoError(t, h.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count))
	require.Zero(t, count, "reader must not commit caller transaction")
}

func TestReadRetryCacheBoundariesSQLite(t *testing.T) {
	for _, mode := range []string{"cache failure", "lookup miss", "cached hook", "panic", "canceled panic"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)")
			native, err := afs.NewCache(t.TempDir(), time.Minute, "boundary", nil)
			require.NoError(t, err)
			const query = "SELECT id FROM records ORDER BY id"
			if mode == "cached hook" {
				writer, err := read.New(context.Background(), h.DB, query, func() any { return &lookupRow{} }, read.WithCache(native))
				require.NoError(t, err)
				require.NoError(t, writer.QueryAll(context.Background(), func(any) error { return nil }))
				require.NoError(t, writer.Stmt().Close())
				h.Exec(t, "DROP TABLE records")
			}
			calls, reconnects, hooks := 0, 0, 0
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "query" {
					calls++
				}
				return nil
			})
			options := []read.Option{read.WithRetry(retryPolicy(db, &reconnects)), read.WithCache(native)}
			if mode == "cache failure" {
				options = append(options, read.WithCache(retryCacheFailure{native}))
			}
			if mode == "lookup miss" || mode == "cached hook" {
				options = append(options, read.WithCacheOnly(true))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader, err := read.New(ctx, db, query, func() any { return &lookupRow{} }, options...)
			require.NoError(t, err)
			invoke := func() {
				err = reader.QueryAll(ctx, func(any) error {
					hooks++
					if mode == "canceled panic" {
						cancel()
					}
					if strings.Contains(mode, "panic") {
						panic("callback panic")
					}
					return invalidReadConnection
				})
			}
			if strings.Contains(mode, "panic") {
				require.PanicsWithValue(t, "callback panic", invoke)
			} else {
				invoke()
			}
			if reader.Stmt() != nil {
				require.NoError(t, reader.Stmt().Close())
			}
			require.Zero(t, reconnects)
			switch mode {
			case "cache failure":
				require.ErrorIs(t, err, invalidReadConnection)
				require.Zero(t, calls)
				require.Zero(t, hooks)
			case "lookup miss":
				require.ErrorIs(t, err, cache.ErrMiss)
				require.Zero(t, calls)
				require.Zero(t, hooks)
			case "cached hook":
				require.ErrorIs(t, err, read.ErrRetryUnsafe)
				require.Zero(t, calls)
				require.Equal(t, 1, hooks)
			default:
				require.Equal(t, 1, calls)
				require.Equal(t, 1, hooks)
			}
			// Reader and writer leases remain usable, including canceled panic
			// cleanup. The completed cached entry must survive a consumer error.
			writer, err := read.New(context.Background(), h.DB, query, func() any { return &lookupRow{} }, read.WithCache(native))
			require.NoError(t, err)
			values := []int{}
			require.NoError(t, writer.QueryAll(context.Background(), func(row any) error { values = append(values, row.(*lookupRow).ID); return nil }))
			require.Equal(t, []int{1, 2}, values)
			if writer.Stmt() != nil {
				require.NoError(t, writer.Stmt().Close())
			}
		})
	}
}

func TestReadRetrySkippedRowsAreNotReplayableSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)")
	queries, reconnects, skipped, emitted := 0, 0, 0, 0
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" {
			queries++
		}
		if call.Phase == "next" && call.Row == 2 {
			return invalidReadConnection
		}
		return nil
	})
	matcher := &cache.ParmetrizedQuery{By: "id", In: []any{1, 2}, Offset: 1, OnSkip: func([]any) error { skipped++; return nil }}
	reader, err := read.New(context.Background(), db, "SELECT id FROM records ORDER BY id", func() any { return &lookupRow{} }, read.WithInMatcher(matcher), read.WithRetry(retryPolicy(db, &reconnects)))
	require.NoError(t, err)
	err = reader.QueryAll(context.Background(), func(any) error { emitted++; return nil })
	require.ErrorIs(t, err, read.ErrRetryUnsafe)
	require.ErrorIs(t, err, invalidReadConnection)
	require.Equal(t, 1, queries)
	require.Zero(t, reconnects)
	require.Zero(t, emitted)
	require.Equal(t, 1, skipped)
	require.NoError(t, reader.Stmt().Close())
}
