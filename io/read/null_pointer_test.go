package read_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	afsoption "github.com/viant/afs/option"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache/afs"
)

func TestReaderCASTNullableIntSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(z TEXT,a INTEGER,b TEXT)",
		"INSERT INTO records VALUES('z',1,'b'),('null',NULL,'n'),('zero',0,'n'),('null-again',NULL,'n')",
	)
	type row struct {
		Z string `sqlx:"z"`
		A *int   `sqlx:"a"`
		B string `sqlx:"b"`
	}
	one, zero := 1, 0
	sqlite.AssertRows(t, h, "SELECT z,a,b FROM records ORDER BY rowid", []row{
		{"z", &one, "b"}, {"null", nil, "n"}, {"zero", &zero, "n"}, {"null-again", nil, "n"},
	})
}

func TestReaderNullableScalarKindsSQLite(t *testing.T) {
	t.Run("int", testReaderNullable[int])
	t.Run("int8", testReaderNullable[int8])
	t.Run("int16", testReaderNullable[int16])
	t.Run("int32", testReaderNullable[int32])
	t.Run("int64", testReaderNullable[int64])
	t.Run("uint", testReaderNullable[uint])
	t.Run("uint8", testReaderNullable[uint8])
	t.Run("uint16", testReaderNullable[uint16])
	t.Run("uint32", testReaderNullable[uint32])
	t.Run("uint64", testReaderNullable[uint64])
	t.Run("float32", testReaderNullable[float32])
	t.Run("float64", testReaderNullable[float64])
	t.Run("string", testReaderNullable[string])
	t.Run("bool", testReaderNullable[bool])
	t.Run("time", testReaderNullable[time.Time])
}

func testReaderNullable[T comparable](t *testing.T) {
	type row struct {
		Value *T `sqlx:"value"`
	}
	for _, mode := range []string{"direct", "reuse", "cache", "cache reuse"} {
		t.Run(mode, func(t *testing.T) {
			statements := []string{"CREATE TABLE records(value)", "INSERT INTO records VALUES(NULL),(1),(NULL),(0),(NULL),(1)"}
			if reflect.TypeOf((*T)(nil)).Elem().Kind() == reflect.String {
				statements = []string{"CREATE TABLE records(value TEXT)", "INSERT INTO records VALUES(NULL),('one'),(NULL),(''),(NULL),('one')"}
			}
			if reflect.TypeOf((*T)(nil)).Elem() == reflect.TypeOf(time.Time{}) {
				statements = []string{"CREATE TABLE records(value DATETIME)", "INSERT INTO records VALUES(NULL),('2026-01-01 00:00:00'),(NULL),('0001-01-01 00:00:00'),(NULL),('2026-01-01 00:00:00')"}
			}
			h := sqlite.New(t, statements...)
			ctx := context.Background()
			standardRows, err := h.DB.QueryContext(ctx, "SELECT value FROM records ORDER BY rowid")
			if err != nil {
				t.Fatal(err)
			}
			var expected []*T
			for standardRows.Next() {
				var value *T
				if err := standardRows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				expected = append(expected, value)
			}
			if err := standardRows.Err(); err != nil {
				t.Fatal(err)
			}
			_ = standardRows.Close()
			options := []read.Option{read.WithMapperCache(read.NewMapperCache(8))}
			cached := mode == "cache" || mode == "cache reuse"
			if cached {
				dataCache, err := afs.NewCache(t.TempDir(), time.Minute, "", afsoption.NewStream(1024*1024, 1024))
				if err != nil {
					t.Fatal(err)
				}
				options = append(options, read.WithCache(dataCache))
			}
			reused := &row{}
			reader, err := read.New(ctx, h.DB, "SELECT value FROM records ORDER BY rowid", func() any {
				if mode == "reuse" || mode == "cache reuse" {
					return reused
				}
				return &row{}
			}, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if statement := reader.Stmt(); statement != nil {
					_ = statement.Close()
				}
			}()
			var first []*T
			for pass := 0; pass < 2; pass++ {
				var actual []*T
				err = reader.QueryAll(ctx, func(value any) error {
					pointer := value.(*row).Value
					if pointer == nil {
						actual = append(actual, nil)
					} else {
						copy := *pointer
						actual = append(actual, &copy)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
				if len(actual) != 6 {
					t.Fatalf("pass %d: got %d rows", pass, len(actual))
				}
				for i, pointer := range actual {
					if (pointer == nil) != (i%2 == 0) {
						t.Errorf("pass %d row %d: nil=%v, want %v", pass, i, pointer == nil, i%2 == 0)
					}
				}
				if !reflect.DeepEqual(actual, expected) {
					t.Errorf("pass %d differs from database/sql nullable scan", pass)
				}
				if pass == 0 {
					first = actual
					if cached {
						h.Exec(t, "DELETE FROM records")
					}
				} else if !reflect.DeepEqual(actual, first) {
					t.Error("repeated query changed NULL/non-NULL values")
				}
			}
		})
	}
}

func TestReaderNullableEmbeddedSQLite(t *testing.T) {
	type Fields struct {
		Value *int `sqlx:"value"`
	}
	type inline struct {
		Ignored int `sqlx:"-"`
		Fields
	}
	type pointer struct {
		Ignored int `sqlx:"-"`
		*Fields
	}
	h := sqlite.New(t, "CREATE TABLE records(value INTEGER)", "INSERT INTO records VALUES(NULL),(1),(NULL),(0),(NULL)")
	one, zero := 1, 0
	sqlite.AssertRows(t, h, "SELECT value FROM records ORDER BY rowid", []inline{
		{Fields: Fields{}}, {Fields: Fields{&one}}, {Fields: Fields{}}, {Fields: Fields{&zero}}, {Fields: Fields{}},
	})
	sqlite.AssertRows(t, h, "SELECT value FROM records ORDER BY rowid", []pointer{
		{Fields: &Fields{}}, {Fields: &Fields{&one}}, {Fields: &Fields{}}, {Fields: &Fields{&zero}}, {Fields: &Fields{}},
	})
}

func TestReaderStandardNullTypesSQLite(t *testing.T) {
	h := sqlite.New(t,
		"CREATE TABLE records(i INTEGER,s TEXT,b BOOLEAN,f REAL,tm DATETIME)",
		"INSERT INTO records VALUES(NULL,NULL,NULL,NULL,NULL),(0,'',false,0,'2026-01-01 00:00:00'),(NULL,NULL,NULL,NULL,NULL)",
	)
	type row struct {
		I  sql.NullInt64   `sqlx:"i"`
		S  sql.NullString  `sqlx:"s"`
		B  sql.NullBool    `sqlx:"b"`
		F  sql.NullFloat64 `sqlx:"f"`
		TM sql.NullTime    `sqlx:"tm"`
	}
	sqlite.AssertRows(t, h, "SELECT * FROM records ORDER BY rowid", []row{{}, {
		I: sql.NullInt64{Valid: true}, S: sql.NullString{Valid: true}, B: sql.NullBool{Valid: true}, F: sql.NullFloat64{Valid: true},
		TM: sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
	}, {}})
}

func TestReaderPointerAndScalarScanSemanticsSQLite(t *testing.T) {
	h := sqlite.New(t, "CREATE TABLE records(value INTEGER)", "INSERT INTO records VALUES(NULL),(0),(1)")
	type pointerRow struct {
		Value *int `sqlx:"value"`
	}
	_, bind, err := io.StructColumnMapper(reflect.TypeOf(pointerRow{}))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := read.New(context.Background(), h.DB, "SELECT value FROM records ORDER BY rowid", func() any { return &pointerRow{} })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if reader.Stmt() != nil {
			_ = reader.Stmt().Close()
		}
	}()
	var actual []driver.Value
	err = reader.QueryAll(context.Background(), func(row any) error {
		args := make([]any, 1)
		bind(row, args, 0, 1)
		value, err := driver.DefaultParameterConverter.ConvertValue(args[0])
		actual = append(actual, value)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, []driver.Value{nil, int64(0), int64(1)}) {
		t.Fatalf("writer bindings lost pointer nullability: %#v", actual)
	}
	type scalarRow struct {
		Value int `sqlx:"value"`
	}
	sqlite.AssertRows(t, h, "SELECT value FROM records WHERE value IS NOT NULL ORDER BY rowid", []scalarRow{{0}, {1}})
	scalar, err := read.New(context.Background(), h.DB, "SELECT value FROM records WHERE value IS NULL", func() any { return &scalarRow{} })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if scalar.Stmt() != nil {
			_ = scalar.Stmt().Close()
		}
	}()
	err = scalar.QueryAll(context.Background(), func(any) error { t.Error("NULL must not emit an int row"); return nil })
	if err == nil || !strings.Contains(err.Error(), "converting NULL to int is unsupported") {
		t.Fatalf("changed scalar NULL scan semantics: %v", err)
	}
}
