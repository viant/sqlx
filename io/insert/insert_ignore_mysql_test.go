package insert_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/insert"
	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

type mysqlIgnoredRow struct {
	Subscription int    `sqlx:"report_suite_subscription_id"`
	Campaign     int    `sqlx:"campaign_id"`
	Note         string `sqlx:"note"`
	Optional     *int   `sqlx:"optional_value"`
}

type mysqlIgnoredSnapshot struct {
	ID, Subscription, Campaign int64
	Note                       string
	Optional                   sql.NullInt64
	Updated                    string
}

type mysqlIgnoredOutcome struct {
	Rows                     []mysqlIgnoredSnapshot
	Warnings                 []int
	Affected, LastID, NextID int64
}

// This test is opt-in independently of allocator acceptance fixtures: it needs
// only disposable tables in the explicitly supplied database, and never changes
// product tables, global SQL mode, or allocator metadata.
func TestMySQLInsertIgnoreMatchesLiteral(t *testing.T) {
	dsn := os.Getenv("SQLX_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SQLX_TEST_MYSQL_DSN absent; MySQL INSERT IGNORE comparison not executed")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("invalid configured MySQL DSN")
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		t.Fatal("cannot connect to configured MySQL database")
	}
	var token [8]byte
	if _, err = rand.Read(token[:]); err != nil {
		conn.Close()
		db.Close()
		t.Fatal(err)
	}
	prefix := "sqlx_ignore_" + hex.EncodeToString(token[:])
	names := []string{prefix + "_audit", prefix + "_literal", prefix + "_service"}
	quote := func(name string) string { return "`" + name + "`" }
	var originalMode string
	if err = conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&originalMode); err != nil {
		conn.Close()
		db.Close()
		t.Fatal(err)
	}
	// Register cleanup before any DDL, including partial fixture setup failures.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := conn.ExecContext(cleanup, "SET SESSION sql_mode = ?", originalMode); err != nil {
			t.Errorf("restore test connection SQL mode: %v", err)
		}
		for i := len(names) - 1; i >= 0; i-- {
			if _, err := conn.ExecContext(cleanup, "DROP TABLE IF EXISTS "+quote(names[i])); err != nil {
				t.Errorf("drop owned test table %s: %v", names[i], err)
			}
		}
		if err := conn.Close(); err != nil {
			t.Errorf("close test connection: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	// Strict mode ensures the truncation is specifically downgraded by IGNORE.
	_, err = conn.ExecContext(ctx, "SET SESSION sql_mode = 'STRICT_ALL_TABLES'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE TABLE "+quote(names[0])+" (mode VARCHAR(16) NOT NULL) ENGINE=InnoDB")
	require.NoError(t, err)
	const originalTime = "2001-01-01 00:00:00.000000"
	for _, table := range names[1:] {
		_, err = conn.ExecContext(ctx, "CREATE TABLE "+quote(table)+" (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, report_suite_subscription_id INT NOT NULL, campaign_id INT NOT NULL, note VARCHAR(4) NOT NULL, optional_value INT NULL, updated DATETIME(6) NOT NULL DEFAULT '2001-01-01 00:00:00.000000' ON UPDATE CURRENT_TIMESTAMP(6), UNIQUE KEY pair_idx(report_suite_subscription_id,campaign_id)) ENGINE=InnoDB")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TRIGGER "+quote(table+"_update")+" AFTER UPDATE ON "+quote(table)+" FOR EACH ROW INSERT INTO "+quote(names[0])+"(mode) VALUES ('"+table[len(prefix)+1:]+"')")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "INSERT INTO "+quote(table)+"(report_suite_subscription_id,campaign_id,note,optional_value) VALUES(31,10,'seed',7)")
		require.NoError(t, err)
	}
	zero, nine := 0, 9
	rows := []*mysqlIgnoredRow{{31, 10, "over", nil}, {31, 20, "new", nil}, {31, 30, "toolong", &zero}, {31, 40, "last", &nine}}
	run := func(table, mode string) mysqlIgnoredOutcome {
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		var outcome mysqlIgnoredOutcome
		if mode == "service" {
			service, err := insert.New(ctx, db, table, registry.LookupDialect(mysqlmeta.MySQL5()), option.InsertIgnore(true))
			require.NoError(t, err)
			outcome.Affected, outcome.LastID, err = service.Exec(ctx, rows, tx, option.BatchSize(len(rows)))
			require.NoError(t, err)
		} else {
			var args []any
			for _, row := range rows {
				args = append(args, row.Subscription, row.Campaign, row.Note, row.Optional)
			}
			result, err := tx.ExecContext(ctx, "INSERT IGNORE INTO "+quote(table)+"(report_suite_subscription_id,campaign_id,note,optional_value) VALUES (?,?,?,?),(?,?,?,?),(?,?,?,?),(?,?,?,?)", args...)
			require.NoError(t, err)
			outcome.Affected, err = result.RowsAffected()
			require.NoError(t, err)
			outcome.LastID, err = result.LastInsertId()
			require.NoError(t, err)
		}
		// Read diagnostics immediately on the same connection, before another SQL
		// statement can replace the warning list. Text includes table names, so
		// compare ordered warning codes rather than fixture-specific messages.
		warnings, err := tx.QueryContext(ctx, "SHOW WARNINGS")
		require.NoError(t, err)
		for warnings.Next() {
			var level, message string
			var code int
			require.NoError(t, warnings.Scan(&level, &code, &message))
			require.Equal(t, "Warning", level)
			outcome.Warnings = append(outcome.Warnings, code)
		}
		require.NoError(t, warnings.Err())
		require.NoError(t, warnings.Close())
		require.Contains(t, outcome.Warnings, 1062, "duplicate must retain INSERT IGNORE warning semantics")
		require.Contains(t, outcome.Warnings, 1265, "bounded string truncation must be an IGNORE warning")
		result, err := tx.QueryContext(ctx, "SELECT id,report_suite_subscription_id,campaign_id,note,optional_value,DATE_FORMAT(updated,'%Y-%m-%d %H:%i:%s.%f') FROM "+quote(table)+" ORDER BY id")
		require.NoError(t, err)
		for result.Next() {
			var row mysqlIgnoredSnapshot
			require.NoError(t, result.Scan(&row.ID, &row.Subscription, &row.Campaign, &row.Note, &row.Optional, &row.Updated))
			outcome.Rows = append(outcome.Rows, row)
		}
		require.NoError(t, result.Err())
		require.NoError(t, result.Close())
		require.Len(t, outcome.Rows, 4)
		require.Equal(t, mysqlIgnoredSnapshot{1, 31, 10, "seed", sql.NullInt64{Int64: 7, Valid: true}, originalTime}, outcome.Rows[0], "existing duplicate and updated timestamp must remain untouched")
		require.Equal(t, int64(20), outcome.Rows[1].Campaign)
		require.False(t, outcome.Rows[1].Optional.Valid)
		require.Equal(t, int64(30), outcome.Rows[2].Campaign)
		require.Equal(t, "tool", outcome.Rows[2].Note)
		require.Equal(t, sql.NullInt64{Valid: true}, outcome.Rows[2].Optional)
		require.Equal(t, int64(40), outcome.Rows[3].Campaign)
		require.Equal(t, sql.NullInt64{Int64: 9, Valid: true}, outcome.Rows[3].Optional)
		require.Equal(t, int64(3), outcome.Affected)
		var updates int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(names[0])+" WHERE mode=?", mode).Scan(&updates))
		require.Zero(t, updates, "INSERT IGNORE must not run UPDATE triggers")
		// A following ordinary insert proves the same native AUTO_INCREMENT
		// consumption as literal SQL, including IDs consumed by ignored rows.
		next, err := tx.ExecContext(ctx, "INSERT INTO "+quote(table)+"(report_suite_subscription_id,campaign_id,note) VALUES(31,50,'next')")
		require.NoError(t, err)
		outcome.NextID, err = next.LastInsertId()
		require.NoError(t, err)
		require.Greater(t, outcome.NextID, outcome.Rows[len(outcome.Rows)-1].ID)
		// Validate that the observation trigger really fires; roll back both this
		// control update and every inserted row through the caller-owned transaction.
		_, err = tx.ExecContext(ctx, "UPDATE "+quote(table)+" SET note='ctrl' WHERE campaign_id=10")
		require.NoError(t, err)
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(names[0])+" WHERE mode=?", mode).Scan(&updates))
		require.Equal(t, 1, updates)
		require.NoError(t, tx.Rollback(), "service must leave caller completion available")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(table)).Scan(&count))
		require.Equal(t, 1, count, "caller rollback must leave only the original seed")
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(names[0])).Scan(&count))
		require.Zero(t, count, "control UPDATE trigger side effects roll back too")
		return outcome
	}
	literal := run(names[1], "literal")
	service := run(names[2], "service")
	require.Equal(t, literal, service, fmt.Sprintf("service must match original literal INSERT IGNORE on %d input rows", len(rows)))
	require.Equal(t, "toolong", rows[2].Note, "binding must not rewrite source values")
	require.Same(t, &zero, rows[2].Optional)
}
