package mysql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestMetadataLockNameLimit(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run MySQL lock metadata integration")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	meta := metadata.New()

	const table = "sequence_records"
	const limit = 64
	boundarySchema := strings.Repeat("s", limit-len(table)-1)
	longSchema := strings.Repeat("s", limit)
	longUnicodeSchema := strings.Repeat("é", len(boundarySchema)+1)
	limitsNames := mysqlLimitsLockNames(ctx, t, db, longSchema+"."+table)
	product := MySQL5()
	lockNames := map[string]struct{}{}
	for _, tc := range []struct {
		schema string
		table  string
	}{
		{"short_schema", table},
		{boundarySchema, table},
		{boundarySchema + "a", table},
		{longSchema, table},
		{longSchema[:limit-1] + "a", table},
		{longSchema, table + "_other"},
		{strings.Repeat("é", len(boundarySchema)), table},
		{longUnicodeSchema, table},
		{longSchema[:limit-1] + "ẞ", table},
		{longSchema[:limit-1] + "ß", table},
	} {
		qualified := tc.schema + "." + tc.table
		t.Run(qualified, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			args := option.NewArgs("", tc.schema, tc.table)
			var first, released, reacquired sink.Lock
			require.NoError(t, meta.Info(ctx, db, info.KindLockGet, &first, args, tx, product))
			require.Equal(t, 1, first.Success)
			require.Equal(t, tc.schema, first.Schema)
			require.Equal(t, tc.table, first.Table)
			require.NotEmpty(t, first.Name)
			if limitsNames && utf8.RuneCountInString(qualified) > limit {
				require.LessOrEqual(t, utf8.RuneCountInString(first.Name), limit)
			} else {
				require.Equal(t, qualified, first.Name)
			}
			require.NotContains(t, lockNames, first.Name)
			lockNames[first.Name] = struct{}{}

			var free int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT IS_FREE_LOCK(?)", first.Name).Scan(&free))
			require.Zero(t, free)
			releaseArgs := args
			if limitsNames && (tc.schema == longSchema || tc.schema == longUnicodeSchema) && tc.table == table {
				releaseArgs = option.NewArgs("", strings.ToUpper(tc.schema), strings.ToUpper(tc.table))
			}
			require.NoError(t, meta.Info(ctx, db, info.KindLockRelease, &released, releaseArgs, tx, product))
			require.Equal(t, first.Name, released.Name)
			require.Equal(t, 1, released.Success)
			require.NoError(t, db.QueryRowContext(ctx, "SELECT IS_FREE_LOCK(?)", first.Name).Scan(&free))
			require.Equal(t, 1, free)
			require.NoError(t, meta.Info(ctx, db, info.KindLockGet, &reacquired, args, tx, product))
			require.Equal(t, first, reacquired)
			require.NoError(t, meta.Info(ctx, db, info.KindLockRelease, &released, args, tx, product))
			require.Equal(t, first, released)
		})
	}
}

func mysqlLimitsLockNames(ctx context.Context, t *testing.T, db *sql.DB, longName string) bool {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var success int
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", longName).Scan(&success)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Contains(t, []uint16{3057, 4163}, mysqlErr.Number)
		return true
	}
	require.Equal(t, 1, success)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", longName).Scan(&success))
	require.Equal(t, 1, success)
	return false
}
