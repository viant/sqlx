package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestProductRegistration(t *testing.T) {
	registered := registry.Products()["clickhouse"]
	if registered == nil || registered.Name != productName {
		t.Fatalf("ClickHouse product is not registered: %+v", registered)
	}
	dialect := registry.LookupDialect(registered)
	if dialect == nil || dialect.Transactional || dialect.Upsert != 0 || dialect.Placeholder != "?" {
		t.Fatalf("unexpected ClickHouse dialect: %+v", dialect)
	}
	identifier, err := dialect.TableIdentifier("driver_smoke", "sqlx_test")
	if err != nil || identifier != `"sqlx_test"."driver_smoke"` {
		t.Fatalf("TableIdentifier() = %q, %v", identifier, err)
	}
}

func TestClickHouseIntegration(t *testing.T) {
	dsn := os.Getenv("SQLX_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("set SQLX_CLICKHOUSE_DSN to run against ClickHouse")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	schemaName := strings.TrimPrefix(parsed.Path, "/")
	if schemaName == "" {
		t.Fatal("SQLX_CLICKHOUSE_DSN must select a database")
	}
	username := ""
	if parsed.User != nil {
		username = parsed.User.Username()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	tableName := fmt.Sprintf("sqlx_integration_%d", time.Now().UnixNano())
	table, err := registry.LookupDialect(ClickHouse()).TableIdentifier(tableName, schemaName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id UInt32, name String, tags Array(UInt32), optional Nullable(String), event_at DateTime) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("drop integration table: %v", err)
		}
	})

	service := metadata.New()
	detected, err := service.DetectProduct(ctx, db)
	if err != nil || detected.Name != productName {
		t.Fatalf("DetectProduct() = %+v, %v", detected, err)
	}
	var schemas []sink.Schema
	if err := service.Info(ctx, db, info.KindSchemas, &schemas); err != nil {
		t.Fatal(err)
	}
	if !containsSchema(schemas, schemaName) {
		t.Fatalf("%s missing from schemas: %+v", schemaName, schemas)
	}
	var currentSchema string
	if err := service.Info(ctx, db, info.KindCurrentSchema, &currentSchema); err != nil || currentSchema != schemaName {
		t.Fatalf("current schema = %q, %v", currentSchema, err)
	}
	var session sink.Session
	if err := service.Info(ctx, db, info.KindSession, &session); err != nil || username != "" && session.Username != username {
		t.Fatalf("session = %+v, %v", session, err)
	}
	var tables []sink.Table
	if err := service.Info(ctx, db, info.KindTables, &tables, option.NewArgs("", schemaName)); err != nil {
		t.Fatal(err)
	}
	if !containsTable(tables, tableName) {
		t.Fatalf("%s missing from tables: %+v", tableName, tables)
	}
	var columns []sink.Column
	if err := service.Info(ctx, db, info.KindTable, &columns, option.NewArgs("", schemaName, tableName)); err != nil {
		t.Fatal(err)
	}
	if !containsColumn(columns, "id", "UInt32") || !containsColumn(columns, "name", "String") ||
		!containsColumn(columns, "tags", "Array(UInt32)") || !containsColumn(columns, "optional", "Nullable(String)") ||
		!containsColumn(columns, "event_at", "DateTime") {
		t.Fatalf("unexpected %s columns: %+v", tableName, columns)
	}
	type record struct {
		ID       uint32    `sqlx:"id"`
		Name     string    `sqlx:"name"`
		Tags     []uint32  `sqlx:"tags"`
		Optional *string   `sqlx:"optional"`
		EventAt  time.Time `sqlx:"event_at"`
	}
	eventAt := time.Date(2026, time.November, 5, 12, 0, 0, 0, time.UTC)
	optional := "present"
	writer, err := insert.New(ctx, db, table, option.BatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if affected, _, err := writer.Exec(ctx, []*record{
		{ID: 7, Name: "clickhouse", Tags: []uint32{1, 2}, Optional: &optional, EventAt: eventAt},
		{ID: 8, Name: "batch", Tags: []uint32{}, EventAt: eventAt},
		{ID: 9, Name: "remainder", Tags: []uint32{3}, EventAt: eventAt},
	}); err != nil || affected != 3 {
		t.Fatalf("insert affected %d rows: %v", affected, err)
	}
	var count uint64
	if err := db.QueryRowContext(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil || count != 3 {
		t.Fatalf("inserted row count = %d, %v", count, err)
	}
	reader, err := read.New(ctx, db, "SELECT id, name, tags, optional, event_at FROM "+table+" WHERE id = ?", func() interface{} { return &record{} })
	if err != nil {
		t.Fatal(err)
	}
	var actual []record
	if err := reader.QueryAll(ctx, func(row interface{}) error {
		actual = append(actual, *row.(*record))
		return nil
	}, uint32(7)); err != nil {
		t.Fatal(err)
	}
	if len(actual) != 1 || actual[0].ID != 7 || actual[0].Name != "clickhouse" ||
		!reflect.DeepEqual(actual[0].Tags, []uint32{1, 2}) || actual[0].Optional == nil ||
		*actual[0].Optional != optional || !actual[0].EventAt.Equal(eventAt) {
		t.Fatalf("unexpected rows: %+v", actual)
	}
	if err := reader.QuerySingle(ctx, func(row interface{}) error {
		actual = append(actual, *row.(*record))
		return nil
	}, uint32(7)); err != nil {
		t.Fatal(err)
	}
	if len(actual) != 2 || !reflect.DeepEqual(actual[1], actual[0]) {
		t.Fatalf("unexpected single row: %+v", actual)
	}
}

func containsSchema(schemas []sink.Schema, name string) bool {
	for _, schema := range schemas {
		if strings.EqualFold(schema.Name, name) {
			return true
		}
	}
	return false
}

func containsTable(tables []sink.Table, name string) bool {
	for _, table := range tables {
		if strings.EqualFold(table.Name, name) {
			return true
		}
	}
	return false
}

func containsColumn(columns []sink.Column, name, kind string) bool {
	for _, column := range columns {
		if strings.EqualFold(column.Name, name) && strings.EqualFold(column.Type, kind) {
			return true
		}
	}
	return false
}
