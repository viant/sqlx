package load

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mysqlmeta "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

func TestMySQLLoadTimestamps(t *testing.T) {
	dsn := os.Getenv("SQLX_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("SQLX_TEST_MYSQL_DSN absent")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("invalid configured MySQL DSN")
	}
	defer db.Close()
	// A dedicated single-connection pool preserves the session timezone and
	// temporary table while the loader acquires its own transaction.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var oldZone string
	if err := db.QueryRowContext(ctx, "SELECT @@session.time_zone").Scan(&oldZone); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "SET SESSION time_zone='+00:00'"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, "SET SESSION time_zone=?", oldZone); err != nil {
			t.Errorf("restore timezone: %v", err)
		}
	}()
	table := fmt.Sprintf("sqlx_time_%d", time.Now().UnixNano())
	if _, err := db.ExecContext(ctx, "CREATE TEMPORARY TABLE "+table+" (id INT PRIMARY KEY, ts TIMESTAMP NULL, dt DATETIME NULL, nullable_ts TIMESTAMP NULL, nullable_dt DATETIME NULL, explicit_dt DATETIME NULL) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, "DROP TEMPORARY TABLE "+table)
	type row struct {
		ID         int        `sqlx:"id"`
		TS         time.Time  `sqlx:"ts"`
		DT         time.Time  `sqlx:"dt"`
		NullableTS *time.Time `sqlx:"nullable_ts"`
		NullableDT *time.Time `sqlx:"nullable_dt"`
		ExplicitDT time.Time  `sqlx:"explicit_dt" timeLayout:"2006-01-02 15:04:05"`
	}
	var input []row
	for index, zone := range []*time.Location{time.UTC, time.FixedZone("east", 5*3600+30*60), time.FixedZone("west", -7*3600), time.UTC} {
		value := time.Date(2026, 11, 9, 12, 34, 56, 123456000, zone)
		item := row{ID: index + 1, TS: value, DT: value, NullableTS: &value, NullableDT: &value, ExplicitDT: value}
		if index == 3 {
			item.NullableTS = nil
			item.NullableDT = nil
		}
		input = append(input, item)
	}
	if _, err := NewSession(registry.LookupDialect(mysqlmeta.MySQL5())).Exec(ctx, input, db, table); err != nil {
		t.Fatal(err)
	}
	for _, item := range input {
		var ts, dt, explicit string
		var nullableTS, nullableDT sql.NullString
		// Casts make this independent of the DSN's parseTime/loc settings.
		query := "SELECT CAST(ts AS CHAR), CAST(dt AS CHAR), CAST(nullable_ts AS CHAR), CAST(nullable_dt AS CHAR), CAST(explicit_dt AS CHAR) FROM " + table + " WHERE id=?"
		if err := db.QueryRowContext(ctx, query, item.ID).Scan(&ts, &dt, &nullableTS, &nullableDT, &explicit); err != nil {
			t.Fatal(err)
		}
		want := item.TS.UTC().Truncate(time.Second)
		actual, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.UTC)
		if err != nil || !actual.Equal(want) {
			t.Fatalf("row %d timestamp=%s, want instant %s (parse error %v)", item.ID, ts, want, err)
		}
		if dt != want.Format("2006-01-02 15:04:05") {
			t.Fatalf("row %d datetime=%s, want %s", item.ID, dt, want)
		}
		for _, nullable := range []sql.NullString{nullableTS, nullableDT} {
			if item.NullableTS == nil {
				if nullable.Valid {
					t.Fatalf("row %d expected SQL NULL, got %s", item.ID, nullable.String)
				}
			} else if !nullable.Valid || nullable.String != dt {
				t.Fatalf("row %d nullable timestamp=%v, want %s", item.ID, nullable, dt)
			}
		}
		if explicit != "2026-11-09 12:34:56" {
			t.Fatalf("explicit layout overridden: %s", explicit)
		}
	}

	// An explicit incompatible layout must remain an error, rather than being
	// rewritten or hidden by the loader's informational-note suppression.
	bad := []struct {
		ID int       `sqlx:"id"`
		TS time.Time `sqlx:"ts" timeLayout:"2006-01-02T15:04:05Z07:00"`
	}{{ID: 99, TS: input[0].TS}}
	_, err = NewSession(registry.LookupDialect(mysqlmeta.MySQL5())).Exec(ctx, bad, db, table)
	if err == nil || !strings.Contains(err.Error(), "Warning 1265") {
		t.Fatalf("explicit RFC3339 should retain warning, got %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE id=99").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("invalid timestamp load committed")
	}
}
