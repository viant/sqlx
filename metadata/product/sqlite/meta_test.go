package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestSQLitePrimaryKeyMetadata(t *testing.T) {
	tests := []struct {
		name  string
		ddl   string
		keys  []sink.Key
		flags map[string]string
	}{
		{name: "reversed declared composite", ddl: "CREATE TABLE test (A TEXT, B TEXT, C TEXT, PRIMARY KEY (B, A))", keys: []sink.Key{{Column: "B", Position: 0}, {Column: "A", Position: 1}}, flags: map[string]string{"A": "PRI", "B": "PRI", "C": ""}},
		{name: "standard compound", ddl: "CREATE TABLE test (A TEXT, B TEXT, C TEXT, PRIMARY KEY (A, B))", keys: []sink.Key{{Column: "A", Position: 0}, {Column: "B", Position: 1}}, flags: map[string]string{"A": "PRI", "B": "PRI", "C": ""}},
		{name: "single integer", ddl: "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", keys: []sink.Key{{Column: "id", Position: 0}}, flags: map[string]string{"id": "PRI", "value": ""}},
		{name: "integer autoincrement", ddl: "CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)", keys: []sink.Key{{Column: "id", Position: 0}}, flags: map[string]string{"id": "PRI", "value": ""}},
		{name: "no primary key", ddl: "CREATE TABLE test (A TEXT, B TEXT, C TEXT)", flags: map[string]string{"A": "", "B": "", "C": ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec(tt.ddl)
			require.NoError(t, err)

			ctx := context.Background()
			meta := metadata.New()
			var columns []sink.Column
			err = meta.Info(ctx, db, info.KindTable, &columns, option.NewArgs("", "", "test"))
			require.NoError(t, err)
			require.Len(t, columns, len(tt.flags))
			for _, column := range columns {
				require.Equal(t, tt.flags[column.Name], column.Key, column.Name)
			}

			var keys []sink.Key
			err = meta.Info(ctx, db, info.KindPrimaryKeys, &keys, option.NewArgs("", "", "test"))
			require.NoError(t, err)
			require.Len(t, keys, len(tt.keys))
			for i, want := range tt.keys {
				require.Equal(t, want.Column, keys[i].Column)
				require.Equal(t, want.Position, keys[i].Position)
			}
			var unfiltered []sink.Key
			require.NoError(t, meta.Info(ctx, db, info.KindPrimaryKeys, &unfiltered))
			require.Equal(t, keys, unfiltered)
		})
	}
}
