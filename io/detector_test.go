package io

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestColumnDetectorUnmappedSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE detector (id INTEGER, unknown)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query, failure string
		unmapped             []string
	}{
		{"strict literal", `SELECT '' AS pseudo FROM detector WHERE 1=0`, "pseudo", nil},
		{"unmapped literal", `SELECT id, '' AS pseudo FROM detector WHERE 1=0`, "", []string{"pseudo"}},
		{"unmapped retains physical metadata", `SELECT id, '' AS pseudo FROM detector WHERE 1=0`, "", []string{"id", "pseudo"}},
		{"unmapped null", `SELECT id, NULL AS pseudo FROM detector WHERE 1=0`, "", []string{"PSEUDO"}},
		{"unknown physical", `SELECT unknown FROM detector WHERE 1=0`, "unknown", nil},
		{"unrelated unknown", `SELECT id, '' AS pseudo, unknown FROM detector WHERE 1=0`, "unknown", []string{"pseudo"}},
		{"wrong alias", `SELECT '' AS authored FROM detector WHERE 1=0`, "authored", []string{"pseudo"}},
		{"missing physical", `SELECT absent FROM detector WHERE 1=0`, "absent", []string{"absent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns, err := (ColumnDetector{UnmappedColumns: tc.unmapped}).Detect(context.Background(), db, tc.query)
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("expected failure for %s, got %v", tc.failure, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(columns) != 2 || columns[0].Name != "id" || columns[0].Type != "INTEGER" || columns[1].Name != "pseudo" || columns[1].Type != "" {
				t.Fatalf("unexpected metadata: %+v", columns)
			}
			scanType := columns[1].ScanType()
			if scanType != nil && scanType.Kind() == reflect.Pointer {
				scanType = scanType.Elem()
			}
			if scanType != nil && scanType.Kind() != reflect.Interface {
				t.Fatalf("invented pseudo scan type: %v", scanType)
			}
		})
	}
}

func TestDetectColumnsSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE detector (name VARCHAR(32), score INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	columns, err := DetectColumns(context.Background(), db, `SELECT name, score FROM detector`)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(columns))
	}

	name := columns[0]
	if name.Name != "name" || name.Type != "VARCHAR(32)" {
		t.Fatalf("unexpected name column: %#v", name)
	}
	if name.ScanType() != reflect.TypeOf((*string)(nil)) {
		t.Fatalf("expected *string scan type, got %v", name.ScanType())
	}
	if name.TypeDefinition != "*string" {
		t.Fatalf("expected *string type definition, got %q", name.TypeDefinition)
	}
	if !name.IsNullable() {
		t.Fatal("SQLite driver-reported nullable flag was not preserved")
	}
	if name.Length != nil {
		t.Fatalf("SQLite driver does not report declared lengths; expected nil, got %d", *name.Length)
	}

	score := columns[1]
	if score.Name != "score" || score.Type != "INTEGER" {
		t.Fatalf("unexpected score column: %#v", score)
	}
	if score.ScanType() != reflect.TypeOf(int(0)) {
		t.Fatalf("expected int scan type, got %v", score.ScanType())
	}
}

func TestDetectColumnsFallsBackToScanType(t *testing.T) {
	registerDetectorFallbackDriver.Do(func() {
		sql.Register(detectorFallbackDriverName, detectorDriver{})
	})
	db, err := sql.Open(detectorFallbackDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	columns, err := DetectColumns(context.Background(), db, "SELECT derived")
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(columns))
	}
	column := columns[0]
	if column.Name != "derived" || column.Type != "int" {
		t.Fatalf("scan type fallback was not applied: %#v", column)
	}
	if column.ScanType() != reflect.TypeOf(int(0)) {
		t.Fatalf("expected int scan type, got %v", column.ScanType())
	}
	if !column.IsNullable() {
		t.Fatal("nullable metadata was not preserved")
	}
	if column.Length == nil || *column.Length != 8 {
		t.Fatalf("expected length 8, got %v", column.Length)
	}
}

const detectorFallbackDriverName = "sqlx-detector-fallback"

var registerDetectorFallbackDriver sync.Once

type detectorDriver struct{}

func (detectorDriver) Open(string) (driver.Conn, error) {
	return detectorConn{}, nil
}

type detectorConn struct{}

func (detectorConn) Prepare(string) (driver.Stmt, error) {
	return detectorStmt{}, nil
}

func (detectorConn) Close() error { return nil }

func (detectorConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

type detectorStmt struct{}

func (detectorStmt) Close() error { return nil }

func (detectorStmt) NumInput() int { return 0 }

func (detectorStmt) Exec([]driver.Value) (driver.Result, error) { return nil, driver.ErrSkip }

func (detectorStmt) Query([]driver.Value) (driver.Rows, error) { return &detectorRows{}, nil }

type detectorRows struct{}

func (*detectorRows) Columns() []string { return []string{"derived"} }

func (*detectorRows) Close() error { return nil }

func (*detectorRows) Next([]driver.Value) error { return io.EOF }

func (*detectorRows) ColumnTypeScanType(int) reflect.Type { return reflect.TypeOf(int64(0)) }

func (*detectorRows) ColumnTypeNullable(int) (bool, bool) { return true, true }

func (*detectorRows) ColumnTypeLength(int) (int64, bool) { return 8, true }

func TestColumnDetectorDeclaredAndHintedSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE declared(id INTEGER,value)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, query string
		declared    []string
		hint        bool
		fail        bool
	}{
		{"undeclared physical", `SELECT value FROM declared`, nil, false, true},
		{"declared physical", `SELECT value FROM declared`, []string{"value"}, false, false},
		{"declared computed", `WITH c AS (SELECT coalesce(value,7) AS value FROM declared) SELECT value FROM c`, []string{"value"}, false, false},
		{"hints", `SELECT '' AS text,0 AS number FROM declared`, nil, true, false},
		{"hint cannot cover unrelated", `SELECT '' AS text,0 AS number,value FROM declared`, nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detector := ColumnDetector{DeclaredColumns: tc.declared}
			if tc.hint {
				detector.ResolveTypes = func(columns []Column) map[int]reflect.Type {
					if columns[0].Name() != "text" || columns[1].Name() != "number" {
						t.Fatal("metadata labels changed")
					}
					return map[int]reflect.Type{0: reflect.TypeOf(""), 1: reflect.TypeOf(0)}
				}
			}
			columns, err := detector.Detect(context.Background(), db, tc.query)
			if tc.fail {
				if err == nil {
					t.Fatal("unknown output accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, column := range columns {
				if column.Type != "" {
					t.Fatalf("invented database type: %+v", column)
				}
			}
			if tc.hint {
				if columns[0].ScanType() != reflect.TypeOf("") || columns[1].ScanType() != reflect.TypeOf(0) || columns[0].IsNullable() || columns[1].IsNullable() {
					t.Fatal("exact literal hints lost")
				}
			}
		})
	}
}

func TestColumnDetectorDoesNotSampleValues(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = conn.Raw(func(raw any) error {
		return raw.(*sqlite3.SQLiteConn).RegisterFunc("metadata_probe", func() int { calls++; return 7 }, false)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Close(); err != nil {
		t.Fatal(err)
	}
	columns, err := (ColumnDetector{DeclaredColumns: []string{"value"}}).Detect(ctx, db, `SELECT metadata_probe() AS value`)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 || columns[0].Name != "value" || calls != 0 {
		t.Fatalf("metadata sampled data: columns=%v calls=%d", columns, calls)
	}
}
