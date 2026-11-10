package load

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/loption"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

func TestMySQLLoadDiagnostics(t *testing.T) {
	dsn := os.Getenv("SQLX_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SQLX_TEST_MYSQL_DSN absent")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("invalid configured MySQL DSN")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var oldNotes, oldLimit int
	if err := conn.QueryRowContext(ctx, "SELECT @@session.sql_notes, @@session.max_error_count").Scan(&oldNotes, &oldLimit); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, fmt.Sprintf("SET SESSION sql_notes=%d, SESSION max_error_count=%d", oldNotes, oldLimit)); err != nil {
			t.Errorf("restore diagnostic settings: %v", err)
		}
	}()
	type row struct {
		ID     int     `sqlx:"id"`
		Amount float64 `sqlx:"amount"`
		Name   string  `sqlx:"name"`
	}
	for index, test := range []struct {
		name       string
		rows       []row
		limit      int
		wantError  string
		wantAmount string
		wantNote   bool
	}{
		{name: "decimal rounding commits", rows: []row{{1, 1.123456789012345, "ok"}}, limit: 64, wantAmount: "1.12345678901", wantNote: true},
		{name: "truncation rolls back", rows: []row{{1, 1, "long"}}, limit: 64, wantError: "Warning 1265"},
		{name: "duplicate rolls back", rows: []row{{1, 1, "ok"}, {1, 2, "ok"}}, limit: 64, wantError: "Warning 1062"},
		{name: "warning after note rolls back", rows: []row{{1, 1.123456789012345, "ok"}, {1, 2, "ok"}}, limit: 64, wantError: "Warning 1062", wantNote: true},
		{name: "hidden warning rolls back", rows: []row{{1, 1.123456789012345, "ok"}, {1, 2, "ok"}}, limit: 1, wantError: "diagnostics incomplete", wantNote: true},
		{name: "disabled diagnostics rolls back", rows: []row{{1, 1, "ok"}, {1, 2, "ok"}}, limit: 0, wantError: "diagnostics incomplete"},
		{name: "truncated notes roll back", rows: []row{{1, 1.123456789012345, "ok"}, {2, 1.123456789012345, "ok"}}, limit: 1, wantError: "diagnostics incomplete", wantNote: true},
		{name: "no diagnostics with storage disabled commits", rows: []row{{1, 1, "ok"}}, limit: 0, wantAmount: "1.00000000000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION sql_notes=1, SESSION max_error_count=%d", test.limit)); err != nil {
				t.Fatal(err)
			}
			table := fmt.Sprintf("sqlx_load_diag_%d_%d", time.Now().UnixNano(), index)
			if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE "+table+" (id INT PRIMARY KEY, amount DECIMAL(21,11), name VARCHAR(2)) ENGINE=InnoDB"); err != nil {
				t.Fatal(err)
			}
			defer conn.ExecContext(ctx, "DROP TEMPORARY TABLE "+table)
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			_, err = NewSession(registry.LookupDialect(mysqlmeta.MySQL5())).Exec(ctx, test.rows, db, table, loption.WithTransaction(tx))
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("load error = %v, want %s", err, test.wantError)
			}
			if test.wantNote {
				var level, message string
				var code int
				if err := tx.QueryRowContext(ctx, "SHOW WARNINGS").Scan(&level, &code, &message); err != nil {
					t.Fatal(err)
				}
				if level != "Note" || code != 1265 {
					t.Fatalf("expected rounding note, got %s %d", level, code)
				}
			}
			if test.wantError != "" {
				err = tx.Rollback()
			} else {
				err = tx.Commit()
			}
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				t.Fatal(err)
			}
			wantRows := 0
			if test.wantError == "" {
				wantRows = len(test.rows)
			}
			if count != wantRows {
				t.Fatalf("persisted rows = %d, want %d", count, wantRows)
			}
			if test.wantAmount != "" {
				var amount string
				if err := conn.QueryRowContext(ctx, "SELECT amount FROM "+table).Scan(&amount); err != nil {
					t.Fatal(err)
				}
				if amount != test.wantAmount {
					t.Fatalf("stored amount = %s, want %s", amount, test.wantAmount)
				}
			}
		})
	}
}

func TestMySQLLoadLargeRoundingBatches(t *testing.T) {
	dsn := os.Getenv("SQLX_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SQLX_TEST_MYSQL_DSN absent")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("invalid configured MySQL DSN")
	}
	defer db.Close()
	// This dedicated pool has exactly one connection and no concurrent users.
	// Settings and temporary tables therefore survive the loader acquiring it.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var oldNotes, oldLimit int
	if err := db.QueryRowContext(ctx, "SELECT @@session.sql_notes, @@session.max_error_count").Scan(&oldNotes, &oldLimit); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, fmt.Sprintf("SET SESSION sql_notes=%d, SESSION max_error_count=%d", oldNotes, oldLimit)); err != nil {
			t.Errorf("restore settings: %v", err)
		}
	}()
	type row struct {
		ID     int     `sqlx:"id"`
		Amount float64 `sqlx:"amount"`
	}
	for index, test := range []struct {
		name      string
		notes     int
		duplicate bool
	}{
		{name: "1025 rounding rows notes enabled", notes: 1},
		{name: "1025 rounding rows notes disabled", notes: 0},
		{name: "duplicate after 1025 rounding rows", notes: 1, duplicate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("SET SESSION sql_notes=%d, SESSION max_error_count=1024", test.notes)); err != nil {
				t.Fatal(err)
			}
			table := fmt.Sprintf("sqlx_large_diag_%d_%d", time.Now().UnixNano(), index)
			if _, err := db.ExecContext(ctx, "CREATE TEMPORARY TABLE "+table+" (id INT PRIMARY KEY, amount DECIMAL(21,11)) ENGINE=InnoDB"); err != nil {
				t.Fatal(err)
			}
			defer db.ExecContext(ctx, "DROP TEMPORARY TABLE "+table)
			rows := make([]row, 1025)
			for i := range rows {
				rows[i] = row{ID: i + 1, Amount: 1.123456789012345}
			}
			if test.duplicate {
				rows = append(rows, rows[0])
			}
			_, err := NewSession(registry.LookupDialect(mysqlmeta.MySQL5())).Exec(ctx, rows, db, table)
			if test.duplicate {
				if err == nil || !strings.Contains(err.Error(), "Warning 1062") {
					t.Fatalf("expected duplicate warning, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var notes, count, valid int
			if err := db.QueryRowContext(ctx, "SELECT @@session.sql_notes").Scan(&notes); err != nil {
				t.Fatal(err)
			}
			if notes != test.notes {
				t.Fatalf("sql_notes=%d, want %d", notes, test.notes)
			}
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*), COUNT(IF(amount=1.12345678901,1,NULL)) FROM "+table).Scan(&count, &valid); err != nil {
				t.Fatal(err)
			}
			want := 1025
			if test.duplicate {
				want = 0
			}
			if count != want || valid != want {
				t.Fatalf("persisted=%d correctly rounded=%d, want %d", count, valid, want)
			}
		})
	}
}
