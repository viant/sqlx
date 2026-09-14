package schema_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/schema"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestEnsureTableSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	d, err := config.Dialect(ctx, h.DB)
	require.NoError(t, err)
	table := schema.Table{Name: `"job.table"`, Dataset: "main", Columns: []io.Column{io.NewColumn("ID", "", reflect.TypeOf(""), io.WithTag(&io.Tag{PrimaryKey: true}))}}
	svc := &schema.Service{DB: h.DB, Dialect: d}
	// Different service objects/connections share no process-local lock or cache.
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- (&schema.Service{DB: h.DB, Dialect: d}).EnsureTable(ctx, table) }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	h.Exec(t, `INSERT INTO "job.table" VALUES ('kept')`)
	require.NoError(t, svc.EnsureTable(ctx, table))
	var count int
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM "job.table"`).Scan(&count))
	require.Equal(t, 1, count)
	h.DB.SetMaxOpenConns(1)
	h.Exec(t, "PRAGMA query_only=ON")
	require.NoError(t, svc.EnsureTable(ctx, table)) // existing table needs no CREATE rights
	table.Name = "missing"
	require.ErrorContains(t, svc.EnsureTable(ctx, table), "readonly")
	h.Exec(t, "PRAGMA query_only=OFF")
	// A failed attempt is not cached; rights restoration permits a retry.
	require.NoError(t, svc.EnsureTable(ctx, table))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	table.Name = "canceled"
	require.True(t, errors.Is(svc.EnsureTable(canceled, table), context.Canceled))
	table.Name = "bad;DROP TABLE missing"
	require.Error(t, svc.EnsureTable(ctx, table))
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='canceled'`).Scan(&count))
	require.Zero(t, count)
}

func TestExistingTableDoesNotRequireDDLTypeSupport(t *testing.T) {
	h := sqlite.New(t, `CREATE TABLE jobs(ID TEXT)`)
	svc := &schema.Service{DB: h.DB, Dialect: &info.Dialect{Product: database.Product{Name: "ANSI"}}}
	// Standard quoting preserves pre-provisioned-table use without a registered
	// vendor renderer; absent tables still fail closed without native DDL authority.
	require.NoError(t, svc.EnsureTable(context.Background(), schema.Table{Name: "jobs"}))
	err := svc.EnsureTable(context.Background(), schema.Table{Name: "absent", Columns: []io.Column{io.NewColumn("ID", "", reflect.TypeOf(""))}})
	require.ErrorContains(t, err, "table creation unsupported")
}
