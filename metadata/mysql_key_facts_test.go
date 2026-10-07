package metadata_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// These exercise the real registry/preparation/scan/cleanup path with controlled
// rows, not a native MySQL server. Root's independently queried MySQL gate stays open.
func TestMySQLKeyFactsNativeServiceLegacySinkAndQuery(t *testing.T) {
	product := &database.Product{Name: "MySQL", Major: 8, Minor: 0, Release: 33}
	for _, target := range []string{"child_schema", "other_schema"} {
		t.Run(target, func(t *testing.T) {
			s := &blockingMetadataScenario{mode: "success", columns: []string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "ON_UPDATE", "ON_DELETE", "ON_MATCH"}, values: []driver.Value{"fk_parent", "FOREIGN KEY", "", "child_schema", "records", "parent_id", target, "parents", "id", int64(1), int64(1), "RESTRICT", "CASCADE", "NONE"}, started: make(chan struct{})}
			db := openBlockingMetadataDB(t, s)
			defer db.Close()
			var keys []sink.Key
			require.NoError(t, metadata.New().Info(context.Background(), db, info.KindForeignKeys, &keys, product, option.NewArgs("", "child_schema", "records")))
			require.Len(t, keys, 1)
			require.Equal(t, target, keys[0].ReferenceSchema)
			require.Equal(t, 1, keys[0].Position)
			require.Equal(t, 1, keys[0].ConstrainPosition)
			require.Equal(t, "CASCADE", keys[0].OnDelete)
			s.mu.Lock()
			query := s.querySQL
			s.mu.Unlock()
			for _, fragment := range []string{"c.REFERENCED_TABLE_SCHEMA AS REFERENCED_TABLE_SCHEMA", "COALESCE(c.ORDINAL_POSITION,0)", "COALESCE(c.POSITION_IN_UNIQUE_CONSTRAINT,0)", "COALESCE(r.UPDATE_RULE,'')", "COALESCE(r.DELETE_RULE,'')", "COALESCE(r.MATCH_OPTION,'')", "LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r", "r.CONSTRAINT_CATALOG=s.CONSTRAINT_CATALOG", "r.CONSTRAINT_SCHEMA=s.CONSTRAINT_SCHEMA", "r.CONSTRAINT_NAME=s.CONSTRAINT_NAME", "r.TABLE_NAME=c.TABLE_NAME"} {
				require.Contains(t, query, fragment)
			}
			require.NotContains(t, query, "s.CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA")
			require.EqualValues(t, 2, s.paramCount.Load())
			require.EqualValues(t, 1, s.rowsCloseCalls.Load())
			require.EqualValues(t, 1, s.stmtCloseCalls.Load())
		})
	}
}

func TestMySQLKeyFactsUnavailableNewFieldsPreserveLegacyMaterialization(t *testing.T) {
	s := &blockingMetadataScenario{mode: "success", columns: []string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "ON_UPDATE", "ON_DELETE", "ON_MATCH"}, values: []driver.Value{"fk", "FOREIGN KEY", "records", "parent_id", int64(0), int64(0), "", "", ""}, started: make(chan struct{})}
	db := openBlockingMetadataDB(t, s)
	defer db.Close()
	var keys []sink.Key
	require.NoError(t, metadata.New().Info(context.Background(), db, info.KindForeignKeys, &keys, &database.Product{Name: "MySQL", Major: 8}, option.NewArgs("", "main", "records")))
	require.Len(t, keys, 1)
	require.Zero(t, keys[0].Position)
	require.Zero(t, keys[0].ConstrainPosition)
	require.Empty(t, keys[0].OnUpdate)
	// Sentinel values remain unknown authority; the borrower rejects them.
	s = &blockingMetadataScenario{mode: "success", columns: []string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION"}, values: []driver.Value{"PRIMARY", "PRIMARY KEY", "records", "id", int64(1)}, started: make(chan struct{})}
	db2 := openBlockingMetadataDB(t, s)
	defer db2.Close()
	keys = nil
	require.NoError(t, metadata.New().Info(context.Background(), db2, info.KindPrimaryKeys, &keys, &database.Product{Name: "MySQL", Major: 8}, option.NewArgs("", "main", "records")))
	require.Equal(t, 1, keys[0].Position)
	s.mu.Lock()
	query := s.querySQL
	s.mu.Unlock()
	require.Contains(t, query, "COALESCE(c.ORDINAL_POSITION,0)")
}

func TestMySQLKeyFactsErrorCancellationAndUnknownColumnsRemainStrict(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "null_scan", true: "unknown_column"}[unknown], func(t *testing.T) {
			columns := []string{"ORDINAL_POSITION"}
			values := []driver.Value{nil}
			if unknown {
				columns = []string{"UNDECLARED_AUTHORITY"}
				values = []driver.Value{"ignored?"}
			}
			s := &blockingMetadataScenario{mode: "success", columns: columns, values: values, started: make(chan struct{})}
			db := openBlockingMetadataDB(t, s)
			defer db.Close()
			var keys []sink.Key
			err := metadata.New().Info(context.Background(), db, info.KindPrimaryKeys, &keys, &database.Product{Name: "MySQL", Major: 8})
			require.Error(t, err)
			require.EqualValues(t, 1, s.rowsCloseCalls.Load())
			require.EqualValues(t, 1, s.stmtCloseCalls.Load())
		})
	}
	s := &blockingMetadataScenario{mode: "success", columns: []string{"ORDINAL_POSITION"}, values: []driver.Value{int64(1)}, started: make(chan struct{})}
	db := openBlockingMetadataDB(t, s)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var keys []sink.Key
	require.True(t, errors.Is(metadata.New().Info(ctx, db, info.KindPrimaryKeys, &keys, &database.Product{Name: "MySQL", Major: 8}), context.Canceled))
	// The approved added index fields do not enlarge legacy JSON metadata shapes.
	n := int64(2)
	zero := int64(0)
	index := sink.Index{Name: "pair", ColumnCount: &n}
	column := sink.Column{Name: "a", IndexPrefixLength: &zero}
	i, err := json.Marshal(index)
	require.NoError(t, err)
	c, err := json.Marshal(column)
	require.NoError(t, err)
	require.False(t, strings.Contains(string(i), "ColumnCount"))
	require.False(t, strings.Contains(string(c), "IndexPrefixLength"))
}
