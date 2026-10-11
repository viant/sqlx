package io

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestColumnDetectorBorrowsTransactionWithOccupiedPool(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// A transaction-local table proves discovery uses this exact connection.
	if _, err = tx.ExecContext(ctx, "CREATE TEMP TABLE discovery_records (id INTEGER, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	occupied, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	columns, err := (ColumnDetector{Tx: tx}).Detect(ctx, db, "SELECT id, name FROM discovery_records WHERE id = ?", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 2 || columns[0].Name != "id" || columns[1].Name != "name" {
		t.Fatalf("columns: %+v", columns)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO discovery_records VALUES (1, 'pending')"); err != nil {
		t.Fatalf("caller transaction completed: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = (ColumnDetector{Tx: tx}).Detect(ctx, db, "SELECT id FROM discovery_records"); err == nil {
		t.Fatal("completed transaction fell back to pool")
	}
}
