package metadata_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	sqliteproduct "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestIndexFactsNativeSQLiteLegacySinks(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "exclusive-indexes.db")
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()
	for _, query := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,a INTEGER,b TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(a,b)", "CREATE INDEX regular ON records(b)"} {
		_, err = db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	var indexes []sink.Index
	require.NoError(t, metadata.New().Info(ctx, db, info.KindIndexes, &indexes, sqliteproduct.SQLite3(), option.NewArgs("", "main", "records")))
	require.Len(t, indexes, 2)
	for _, idx := range indexes {
		require.NotNil(t, idx.ColumnCount)
		if idx.Name == "pair_unique" {
			require.Equal(t, "1", idx.Unique)
			require.Equal(t, int64(2), *idx.ColumnCount)
		}
		raw, err := json.Marshal(idx)
		require.NoError(t, err)
		var exposed map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &exposed))
		_, has := exposed["ColumnCount"]
		require.False(t, has)
	}
	var members []sink.Column
	require.NoError(t, metadata.New().Info(ctx, db, info.KindIndex, &members, sqliteproduct.SQLite3(), option.NewArgs("", "main", "records", "pair_unique")))
	require.Len(t, members, 2)
	require.Equal(t, "a", members[0].Name)
	require.Equal(t, "b", members[1].Name)
	for _, m := range members {
		require.Nil(t, m.IndexPrefixLength)
		require.Equal(t, "1", m.Key)
	}
	// Preserve a native count even when existing named-member filtering hides an expression.
	_, err = db.ExecContext(ctx, "CREATE UNIQUE INDEX mixed_expr ON records(a,lower(b))")
	require.NoError(t, err)
	indexes = nil
	require.NoError(t, metadata.New().Info(ctx, db, info.KindIndexes, &indexes, sqliteproduct.SQLite3(), option.NewArgs("", "main", "records")))
	for _, idx := range indexes {
		if idx.Name == "mixed_expr" {
			require.Equal(t, int64(2), *idx.ColumnCount)
		}
	}
	members = nil
	require.NoError(t, metadata.New().Info(ctx, db, info.KindIndex, &members, sqliteproduct.SQLite3(), option.NewArgs("", "main", "records", "mixed_expr")))
	require.Len(t, members, 1)
}

func TestIndexFactsMySQLNativeLegacySinksAndRawConvention(t *testing.T) {
	product := &database.Product{Name: "MySQL", Major: 8}
	s := &blockingMetadataScenario{mode: "success", columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_SCHEMA", "INDEX_NAME", "INDEX_TYPE", "INDEX_UNIQUE", "INDEX_COLUMNS", "INDEX_COLUMN_COUNT"}, values: []driver.Value{"", "main", "records", "main", "pair_unique", "BTREE", "0", "a,b", int64(2)}, started: make(chan struct{})}
	db := openBlockingMetadataDB(t, s)
	defer db.Close()
	var indexes []sink.Index
	require.NoError(t, metadata.New().Info(context.Background(), db, info.KindIndexes, &indexes, product, option.NewArgs("", "main", "records")))
	require.Len(t, indexes, 1)
	require.Equal(t, "0", indexes[0].Unique)
	require.Equal(t, int64(2), *indexes[0].ColumnCount)
	s.mu.Lock()
	query := s.querySQL
	s.mu.Unlock()
	require.Contains(t, query, "CASE WHEN NON_UNIQUE = 1 THEN 1 ELSE 0 END AS INDEX_UNIQUE")
	require.Contains(t, query, "COUNT(*) AS INDEX_COLUMN_COUNT")
	s = &blockingMetadataScenario{mode: "success", columns: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "COLUMN_NAME", "COLLATION", "INDEX_POSITION", "INDEX_PREFIX_LENGTH"}, values: []driver.Value{"", "main", "records", "pair_unique", "a", "A", int64(1), int64(0)}, started: make(chan struct{})}
	db2 := openBlockingMetadataDB(t, s)
	defer db2.Close()
	var members []sink.Column
	require.NoError(t, metadata.New().Info(context.Background(), db2, info.KindIndex, &members, product, option.NewArgs("", "main", "records", "pair_unique")))
	require.NotNil(t, members[0].IndexPrefixLength)
	require.Zero(t, *members[0].IndexPrefixLength)
	s.mu.Lock()
	query = s.querySQL
	s.mu.Unlock()
	require.Contains(t, query, "COALESCE(SUB_PART,0) AS INDEX_PREFIX_LENGTH")
	// Unchanged queries/products leave absent fields nil rather than inventing zero.
	s = &blockingMetadataScenario{mode: "success", columns: []string{"TABLE_NAME", "INDEX_NAME", "INDEX_UNIQUE", "INDEX_COLUMNS"}, values: []driver.Value{"records", "pair_unique", "1", "a,b"}, started: make(chan struct{})}
	db3 := openBlockingMetadataDB(t, s)
	defer db3.Close()
	indexes = nil
	require.NoError(t, metadata.New().Info(context.Background(), db3, info.KindIndexes, &indexes, sqliteproduct.SQLite3()))
	require.Nil(t, indexes[0].ColumnCount)
}
