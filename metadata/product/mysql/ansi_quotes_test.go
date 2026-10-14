package mysql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
)

func TestMetadataWithANSIQuotes(t *testing.T) {
	path := os.Getenv("SQLX_MYSQL_TEST_DSN_FILE")
	if path == "" {
		t.Skip("set SQLX_MYSQL_TEST_DSN_FILE to run MySQL metadata integration")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var original string
	if err = conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&original); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "SET SESSION sql_mode = ?", original) }()
	modes := []string{}
	for _, mode := range strings.Split(original, ",") {
		if mode != "ANSI_QUOTES" {
			modes = append(modes, mode)
		}
	}
	for _, ansi := range []bool{false, true} {
		name := "default"
		selected := strings.Join(modes, ",")
		if ansi {
			name = "ANSI_QUOTES"
			selected += ",ANSI_QUOTES"
		}
		t.Run(name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ?", selected); err != nil {
				t.Fatal(err)
			}
			query := registry.Lookup(MySQL5().Name, info.KindSession).Match(MySQL5())
			var pid, user, catalog, schema, app string
			if err := conn.QueryRowContext(ctx, query.SQL).Scan(&pid, &user, &catalog, &schema, &app); err != nil {
				t.Fatal(err)
			}
			if catalog != "" || app != "" || pid == "" || schema == "" {
				t.Fatal("invalid session metadata")
			}
			query = registry.Lookup(MySQL5().Name, info.KindTable).Match(MySQL5())
			rows, err := conn.QueryContext(ctx, query.SQL+" WHERE TABLE_SCHEMA=DATABASE()")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatalf("test database has no columns: %v", rows.Err())
			}
		})
	}
}
