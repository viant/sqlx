package io

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

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
