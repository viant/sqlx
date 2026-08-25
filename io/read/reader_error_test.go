package read

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
)

type readerErrorRecord struct {
	Value string
}

type readerErrorSource struct {
	columns     []sqlxio.Column
	convertErr  error
	checkErr    error
	scannerErr  error
	sourceErr   error
	closeErr    error
	typeMatches bool
	next        bool
	closed      bool
	rolledBack  bool
}

func (s *readerErrorSource) ConvertColumns() ([]sqlxio.Column, error) {
	return s.columns, s.convertErr
}

func (s *readerErrorSource) Scanner(context.Context) cache.ScannerFn {
	return func(...interface{}) error {
		return s.scannerErr
	}
}

func (s *readerErrorSource) XTypes() []*xunsafe.Type {
	return nil
}

func (s *readerErrorSource) CheckType(context.Context, []interface{}) (bool, error) {
	return s.typeMatches, s.checkErr
}

func (s *readerErrorSource) Close(context.Context) error {
	s.closed = true
	return s.closeErr
}

func (s *readerErrorSource) Next() bool {
	if !s.next {
		return false
	}
	s.next = false
	return true
}

func (s *readerErrorSource) Rollback(context.Context) error {
	s.rolledBack = true
	return nil
}

func (s *readerErrorSource) Err() error {
	return s.sourceErr
}

type readerErrorCache struct {
	cache.Cache
	err error
}

func (c *readerErrorCache) Get(context.Context, string, []interface{}, ...interface{}) (*cache.Entry, error) {
	return nil, c.err
}

type prepareErrorDriver struct {
	err error
}

func (d prepareErrorDriver) Open(string) (driver.Conn, error) {
	return &prepareErrorConn{err: d.err}, nil
}

type prepareErrorConn struct {
	err error
}

func (c *prepareErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, c.err
}

func (c *prepareErrorConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, c.err
}

func (c *prepareErrorConn) Close() error {
	return nil
}

func (c *prepareErrorConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func TestReader_ErrorContext(t *testing.T) {
	t.Run("cache entry", func(t *testing.T) {
		cause := errors.New("cache unavailable")
		reader := &Reader{options: options{cache: &readerErrorCache{err: cause}}}

		err := reader.QueryAll(context.Background(), func(interface{}) error { return nil })
		assertWrappedError(t, err, cause, "failed to cache entry")
	})

	t.Run("prepare row", func(t *testing.T) {
		cause := errors.New("columns unavailable")
		source := newReaderErrorSource()
		source.convertErr = cause
		reader := newReaderErrorTestReader(nil)

		err := reader.readAll(context.Background(), func(interface{}) error { return nil }, nil, source)
		assertWrappedError(t, err, cause, "failed to read row: failed to prepare row")
		if !source.rolledBack {
			t.Fatal("expected the source to be rolled back")
		}
	})

	t.Run("emit", func(t *testing.T) {
		cause := errors.New("emit failed")
		source := newReaderErrorSource()
		reader := newReaderErrorTestReader(nil)

		err := reader.readAll(context.Background(), func(interface{}) error { return cause }, nil, source)
		assertWrappedError(t, err, cause, "failed to read row: failed to emit row")
	})

	t.Run("source", func(t *testing.T) {
		cause := errors.New("source failed")
		source := newReaderErrorSource()
		source.next = false
		source.sourceErr = cause
		reader := newReaderErrorTestReader(nil)

		err := reader.readAll(context.Background(), func(interface{}) error { return nil }, nil, source)
		assertWrappedError(t, err, cause, "source err")
	})

	t.Run("EOF closes source", func(t *testing.T) {
		source := newReaderErrorSource()
		source.scannerErr = io.EOF
		reader := newReaderErrorTestReader(nil)

		if err := reader.readAll(context.Background(), func(interface{}) error { return nil }, nil, source); err != nil {
			t.Fatalf("expected EOF to finish the read, got: %v", err)
		}
		if !source.closed {
			t.Fatal("expected the source to be closed")
		}
	})

	t.Run("skip closes source", func(t *testing.T) {
		source := newReaderErrorSource()
		source.scannerErr = SkipError("skip")
		reader := newReaderErrorTestReader(nil)

		if err := reader.readAll(context.Background(), func(interface{}) error { return nil }, nil, source); err != nil {
			t.Fatalf("expected SkipError to finish without error, got: %v", err)
		}
		if !source.closed {
			t.Fatal("expected the source to be closed")
		}
	})
}

func TestReader_CheckType(t *testing.T) {
	t.Run("check error", func(t *testing.T) {
		cause := errors.New("type check failed")
		source := newReaderErrorSource()
		source.checkErr = cause
		reader := newReaderErrorTestReader(nil)
		var mapper RowMapper

		err := reader.read(context.Background(), source, &mapper, func(interface{}) error { return nil }, nil)
		assertWrappedError(t, err, cause, "failed to check cache type")
	})

	t.Run("type mismatch", func(t *testing.T) {
		source := newReaderErrorSource()
		source.typeMatches = false
		reader := newReaderErrorTestReader(nil)
		var mapper RowMapper

		err := reader.read(context.Background(), source, &mapper, func(interface{}) error { return nil }, nil)
		if err == nil || err.Error() != "invalid cache type" {
			t.Fatalf("expected invalid cache type, got: %v", err)
		}
	})
}

func TestReader_PrepareError(t *testing.T) {
	cause := errors.New("prepare failed")
	driverName := "sqlx-reader-prepare-error"
	sql.Register(driverName, prepareErrorDriver{err: cause})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}
	defer db.Close()

	reader := &Reader{options: options{db: db}, query: "SELECT 1"}
	err = reader.QueryAll(context.Background(), func(interface{}) error { return nil })
	assertWrappedError(t, err, cause, "failed to create stmt source: failed to prepare context")
}

func newReaderErrorSource() *readerErrorSource {
	return &readerErrorSource{
		columns: []sqlxio.Column{
			sqlxio.NewColumn("value", "TEXT", reflect.TypeOf("")),
		},
		typeMatches: true,
		next:        true,
	}
}

func newReaderErrorTestReader(mapperErr error) *Reader {
	return NewStmt(nil, func() interface{} {
		return &readerErrorRecord{}
	}, WithRowMapper(func([]sqlxio.Column, reflect.Type, sqlxio.Resolve, ...Option) (RowMapper, error) {
		if mapperErr != nil {
			return nil, mapperErr
		}
		return func(target interface{}) ([]interface{}, error) {
			return []interface{}{&target.(*readerErrorRecord).Value}, nil
		}, nil
	}))
}

func assertWrappedError(t *testing.T, actual, cause error, message string) {
	t.Helper()
	if !errors.Is(actual, cause) {
		t.Fatalf("expected error to wrap %v, got: %v", cause, actual)
	}
	if !strings.Contains(actual.Error(), message) {
		t.Fatalf("expected error %q to contain %q", actual, message)
	}
}
