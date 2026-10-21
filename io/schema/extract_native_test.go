package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

type schemaQuery func(string, []driver.NamedValue) (driver.Rows, error)
type schemaConnector struct{ query schemaQuery }

func (c schemaConnector) Connect(context.Context) (driver.Conn, error) {
	return &schemaConnection{query: c.query}, nil
}
func (c schemaConnector) Driver() driver.Driver { return schemaDriver{} }

type schemaDriver struct{}

func (schemaDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type schemaConnection struct{ query schemaQuery }

func (c *schemaConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unsupported")
}
func (c *schemaConnection) Close() error              { return nil }
func (c *schemaConnection) Begin() (driver.Tx, error) { return schemaTransaction{}, nil }
func (c *schemaConnection) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *schemaConnection) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if q != "SET LOCAL search_path = pg_catalog" {
		return nil, errors.New("unexpected Exec: " + q)
	}
	return driver.RowsAffected(0), nil
}

type schemaTransaction struct{}

func (schemaTransaction) Commit() error   { return nil }
func (schemaTransaction) Rollback() error { return nil }
func (c *schemaConnection) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.query(q, args)
}

type schemaRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *schemaRows) Columns() []string { return r.columns }
func (r *schemaRows) Close() error      { return nil }
func (r *schemaRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
func nativeRows(columns []string, values ...[]driver.Value) driver.Rows {
	return &schemaRows{columns: columns, values: values}
}

func TestNativeMySQLExtraction(t *testing.T) {
	childDDL := "CREATE TABLE `child` (`id` bigint NOT NULL AUTO_INCREMENT, `parent` bigint, PRIMARY KEY (`id`), CONSTRAINT `fk` FOREIGN KEY (`parent`) REFERENCES `parent` (`id`)) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4"
	parentDDL := "CREATE TABLE `parent` (`id` bigint NOT NULL, `quoted.name` decimal(12,4) DEFAULT 1.2345, PRIMARY KEY (`id`)) ENGINE=InnoDB"
	var queries []string
	db := sql.OpenDB(schemaConnector{query: func(q string, args []driver.NamedValue) (driver.Rows, error) {
		queries = append(queries, q)
		switch {
		case strings.HasPrefix(q, "SELECT TABLE_TYPE"):
			require.Equal(t, "test", args[0].Value)
			return nativeRows([]string{"TABLE_TYPE"}, []driver.Value{"BASE TABLE"}), nil
		case q == "SHOW CREATE TABLE `test`.`child`":
			return nativeRows([]string{"Table", "Create Table"}, []driver.Value{"child", childDDL}), nil
		case q == "SHOW CREATE TABLE `test`.`parent`":
			return nativeRows([]string{"Table", "Create Table"}, []driver.Value{"parent", parentDDL}), nil
		case strings.Contains(q, "information_schema.KEY_COLUMN_USAGE"):
			if args[1].Value == "child" {
				return nativeRows([]string{"schema", "table"}, []driver.Value{"test", "parent"}), nil
			}
			return nativeRows([]string{"schema", "table"}), nil
		default:
			t.Fatalf("unexpected query %s", q)
			return nil, nil
		}
	}})
	defer db.Close()
	d := &info.Dialect{Product: database.Product{Name: "MySQL"}}
	a, err := Extract(context.Background(), db, ExtractOptions{Dialect: d, Schema: "test", Tables: []string{"child"}, IncludeDependencies: true})
	require.NoError(t, err)
	require.True(t, a.Complete)
	require.Equal(t, []string{"parent", "child"}, a.Tables)
	require.Equal(t, parentDDL, a.Statements[0])
	require.Contains(t, a.Statements[1], "AUTO_INCREMENT,")
	require.NotContains(t, a.Statements[1], "AUTO_INCREMENT=42")
	require.Len(t, queries, 6)
	a, err = Extract(context.Background(), db, ExtractOptions{Dialect: d, Schema: "test", Tables: []string{"child"}, IncludeDependencies: true, IncludeSequenceState: true})
	require.NoError(t, err)
	require.Equal(t, childDDL, a.Statements[1])
	_, err = Extract(context.Background(), db, ExtractOptions{Dialect: d, Schema: "test", Tables: []string{"child"}})
	require.ErrorContains(t, err, "requires unselected")
	_, err = Apply(context.Background(), db, a, ApplyOptions{Dialect: d, Atomic: true})
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestBigQueryNativeDDLReportsUncertifiedScope(t *testing.T) {
	ddl := "CREATE TABLE `project.dataset.nested` (id INT64 NOT NULL, payload STRUCT<items ARRAY<STRUCT<name STRING, quantity NUMERIC(12,4)>>>) PARTITION BY RANGE_BUCKET(id,GENERATE_ARRAY(0,100,10)) CLUSTER BY id OPTIONS(description='native; nested')"
	db := sql.OpenDB(schemaConnector{query: func(q string, args []driver.NamedValue) (driver.Rows, error) {
		require.Equal(t, "SELECT table_type,ddl FROM `project.dataset.INFORMATION_SCHEMA.TABLES` WHERE table_name = ?", q)
		require.Equal(t, "nested", args[0].Value)
		return nativeRows([]string{"table_type", "ddl"}, []driver.Value{"BASE TABLE", ddl}), nil
	}})
	defer db.Close()
	opts := ExtractOptions{Dialect: &info.Dialect{Product: database.Product{Name: "BigQuery"}}, Schema: "project.dataset", Tables: []string{"nested"}}
	a, err := Extract(context.Background(), db, opts)
	require.ErrorIs(t, err, ErrUnsupported)
	require.False(t, a.Complete)
	require.Equal(t, []string{ddl}, a.Statements)
	opts.AllowPartial = true
	a, err = Extract(context.Background(), db, opts)
	require.NoError(t, err)
	require.False(t, a.Complete)
	require.NotEmpty(t, a.Omissions)
}

func TestNativeCounterAndCycleHandling(t *testing.T) {
	ddl := "CREATE TABLE `weird` (`AUTO_INCREMENT` int AUTO_INCREMENT, s varchar(60) DEFAULT 'AUTO_INCREMENT=99', PRIMARY KEY (`AUTO_INCREMENT`)) ENGINE=InnoDB AUTO_INCREMENT = 42 COMMENT='AUTO_INCREMENT=7'"
	actual, err := withoutMySQLCounter(ddl)
	require.NoError(t, err)
	require.Contains(t, actual, "DEFAULT 'AUTO_INCREMENT=99'")
	require.Contains(t, actual, "COMMENT='AUTO_INCREMENT=7'")
	require.NotContains(t, actual, "AUTO_INCREMENT = 42")
	require.Empty(t, dependencyCycle(map[string]bool{"self": true}, map[string][]string{"self": {"self"}}))
	require.NotEmpty(t, dependencyCycle(map[string]bool{"a": true, "b": true}, map[string][]string{"a": {"b"}, "b": {"a"}}))
}
