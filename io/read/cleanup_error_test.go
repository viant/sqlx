package read

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/viant/sqlx/io/read/cache"
)

type cleanupCause struct{ text string }

func (e *cleanupCause) Error() string { return e.text }

type cleanupDriver struct{ cause error }

func (d cleanupDriver) Open(string) (driver.Conn, error) { return &cleanupConn{cause: d.cause}, nil }

type cleanupConn struct{ cause error }

func (c *cleanupConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *cleanupConn) Close() error              { return nil }
func (c *cleanupConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *cleanupConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &cleanupRows{cause: c.cause}, nil
}

type cleanupRows struct{ cause error }

func (r *cleanupRows) Columns() []string         { return []string{"value"} }
func (r *cleanupRows) Close() error              { return r.cause }
func (r *cleanupRows) Next([]driver.Value) error { return io.EOF }

type cleanupCache struct {
	cache.Cache
	cause error
}

func (c cleanupCache) Close(context.Context, *cache.Entry) error { return c.cause }

func TestCleanupErrorReturnedRowsAndCacheCauses(t *testing.T) {
	ctx := context.Background()
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			rowCause := &cleanupCause{"row close"}
			cacheCause := &cleanupCause{"cache close"}
			driverName := "cleanup_close_" + fmt.Sprint(enabled)
			sql.Register(driverName, cleanupDriver{cause: rowCause})
			db, err := sql.Open(driverName, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			native, err := db.QueryContext(ctx, "SELECT value")
			if err != nil {
				t.Fatal(err)
			}
			source, err := NewRows(native, cleanupCache{cause: cacheCause}, &cache.Entry{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			source.cleanupErrorProvenance = enabled
			err = source.Close(ctx)
			if err == nil || err.Error() != "cache closerow close" {
				t.Fatalf("display changed: %v", err)
			}
			var cleanup *CleanupError
			typed := errors.As(err, &cleanup)
			if typed != enabled {
				t.Fatalf("cleanup marker=%v enabled=%v", typed, enabled)
			}
			if errors.Is(err, rowCause) != enabled || errors.Is(err, cacheCause) != enabled {
				t.Fatalf("cause retention differs: %v", err)
			}
			if enabled {
				var actual *cleanupCause
				if !errors.As(err, &actual) {
					t.Fatal("actual typed cause lost")
				}
			}
		})
	}
}

func TestCleanupErrorReturnedCacheSourceOnly(t *testing.T) {
	cause := &cleanupCause{"source close"}
	for _, enabled := range []bool{false, true} {
		source := &readerErrorSource{closeErr: cause}
		reader := &Reader{options: options{cleanupErrorProvenance: enabled}}
		err := reader.readAll(context.Background(), func(any) error { t.Fatal("empty source emitted row"); return nil }, nil, source)
		if err == nil || err.Error() != cause.Error() || !errors.Is(err, cause) {
			t.Fatalf("actual source error lost: %v", err)
		}
		var cleanup *CleanupError
		if errors.As(err, &cleanup) != enabled {
			t.Fatalf("cleanup marker changed for %v", enabled)
		}
		if !source.closed || !source.rolledBack {
			t.Fatal("existing cleanup flow changed")
		}
	}
	if cleanupError(nil) != nil {
		t.Fatal("invented cleanup error")
	}
	wrapped := cleanupError(cause)
	if cleanupError(wrapped) != wrapped {
		t.Fatal("double-wrapped actual cleanup")
	}
}
