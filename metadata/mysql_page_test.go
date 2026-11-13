package metadata_test

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Uses the native MySQL metadata SQL/preparation/scanning path over a synthetic
// INFORMATION_SCHEMA fixture. A real MySQL transport gate is separate.
func TestMySQLColumnMetadataPagesAreBoundedAndScopeBound(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`ATTACH DATABASE ':memory:' AS INFORMATION_SCHEMA;
 CREATE TABLE INFORMATION_SCHEMA.COLUMNS(TABLE_SCHEMA TEXT,TABLE_NAME TEXT,COLUMN_NAME TEXT,ORDINAL_POSITION INTEGER,COLUMN_COMMENT TEXT,DATA_TYPE TEXT,CHARACTER_MAXIMUM_LENGTH INTEGER,NUMERIC_PRECISION INTEGER,NUMERIC_SCALE INTEGER,IS_NULLABLE TEXT,COLUMN_DEFAULT TEXT,COLUMN_KEY TEXT DEFAULT '',EXTRA TEXT);
 INSERT INTO INFORMATION_SCHEMA.COLUMNS(TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_COMMENT,DATA_TYPE,IS_NULLABLE,EXTRA) VALUES
 ('data','b','id',1,'identity','int','NO',''),('data','a','label',2,'display label','text','YES',''),('data','a','id',1,'identity','int','NO',''),('other','secret','id',1,'other scope','int','NO','');`)
	require.NoError(t, err)
	page := &option.MetadataPage{Limit: 2}
	var first []sink.Column
	err = metadata.New().Info(context.Background(), db, info.KindTable, &first, mysql.MySQL5(), option.NewArgs("", "data", ""), page)
	require.NoError(t, err)
	require.True(t, page.Handled)
	require.Len(t, first, 2)
	require.Equal(t, "a", first[0].Table)
	require.Equal(t, "id", first[0].Name)
	require.Equal(t, "label", first[1].Name)
	require.NotEmpty(t, page.NextToken)
	token := page.NextToken
	page.Token = token
	var second []sink.Column
	require.NoError(t, metadata.New().Info(context.Background(), db, info.KindTable, &second, mysql.MySQL5(), option.NewArgs("", "data", ""), page))
	require.Len(t, second, 1)
	require.Equal(t, "b", second[0].Table)
	require.Empty(t, page.NextToken)
	page.Token = token
	var invalid []sink.Column
	err = metadata.New().Info(context.Background(), db, info.KindTable, &invalid, mysql.MySQL5(), option.NewArgs("", "other", ""), page)
	require.ErrorIs(t, err, metadata.ErrMetadataCursor)
	require.Empty(t, invalid)
	require.False(t, page.Handled)
	page.Token = ""
	err = metadata.New().Info(context.Background(), db, info.KindTable, &invalid, mysql.MySQL5(), option.NewArgs("", "", ""), page)
	require.Error(t, err)
	var complete []sink.Column
	require.NoError(t, metadata.New().Info(context.Background(), db, info.KindTable, &complete, mysql.MySQL5(), option.NewArgs("", "data", "")))
	require.Len(t, complete, 3, "unpaged metadata behavior is preserved")
}

func TestMySQLMetadataPagingLiveFixture(t *testing.T) {
	dsn := os.Getenv("SQLX_METADATA_MYSQL_DSN")
	if dsn == "" {
		t.Skip("isolated MySQL fixture not configured")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema := fmt.Sprintf("metadata_page_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, "CREATE DATABASE `"+schema+"`")
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE `" + schema + "`")
	_, err = db.ExecContext(ctx, "CREATE TABLE `"+schema+"`.first_table(id INT, label VARCHAR(30) COMMENT 'Display label') COMMENT='Records catalog'")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `"+schema+"`.second_table(id INT)")
	require.NoError(t, err)
	tablePage := &option.MetadataPage{Limit: 1}
	var tables []sink.Table
	require.NoError(t, metadata.New().Info(ctx, db, info.KindTables, &tables, mysql.MySQL5(), option.NewArgs("", schema), tablePage))
	require.Len(t, tables, 1)
	require.NotNil(t, tables[0].Comment)
	require.Equal(t, "Records catalog", *tables[0].Comment)
	require.NotEmpty(t, tablePage.NextToken)
	page := &option.MetadataPage{Limit: 2}
	var columns []sink.Column
	require.NoError(t, metadata.New().Info(ctx, db, info.KindTable, &columns, mysql.MySQL5(), option.NewArgs("", schema, ""), page))
	require.Len(t, columns, 2)
	require.Equal(t, "Display label", columns[1].Comments)
	require.NotEmpty(t, page.NextToken)
	page.Token = page.NextToken
	columns = nil
	require.NoError(t, metadata.New().Info(ctx, db, info.KindTable, &columns, mysql.MySQL5(), option.NewArgs("", schema, ""), page))
	require.Len(t, columns, 1)
	require.Equal(t, "second_table", columns[0].Table)
	require.Empty(t, page.NextToken)
}
