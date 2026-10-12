package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/registry"
)

var errUnexpectedVersionConnection = errors.New("fixed BigQuery version attempted a database connection")

type fixedBQVersionDriver struct{ opens atomic.Int32 }

func (d *fixedBQVersionDriver) Open(string) (driver.Conn, error) {
	d.opens.Add(1)
	return nil, errUnexpectedVersionConnection
}

type fixedBQVersionConnector struct{ driver *fixedBQVersionDriver }

func (c *fixedBQVersionConnector) Driver() driver.Driver { return c.driver }
func (c *fixedBQVersionConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}

func fixedVersionDB(t *testing.T) (*sql.DB, *fixedBQVersionDriver) {
	t.Helper()
	typ := reflect.TypeFor[fixedBQVersionDriver]()
	require.NoError(t, registry.RegisterDriver(*bigquery.BigQuery(), typ.PkgPath(), typ.Name()))
	d := &fixedBQVersionDriver{}
	db := sql.OpenDB(&fixedBQVersionConnector{driver: d})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, d
}

func TestBigQueryFixedVersionDoesNotConnect(t *testing.T) {
	db, d := fixedVersionDB(t)
	// Independent services exercise cold detection rather than the recent cache.
	for range 2 {
		product, err := New().DetectProduct(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, "BigQuery", product.Name)
		require.Zero(t, product.Major)
		require.Zero(t, product.Minor)
		require.Zero(t, product.Release)
	}
	var version string
	require.NoError(t, New().Info(context.Background(), db, info.KindVersion, &version, bigquery.BigQuery()))
	require.Equal(t, "BigQuery 0.0.0", version)
	require.Zero(t, d.opens.Load())
	// Other sink shapes retain the registered SQL fallback.
	var versions []string
	err := New().Info(context.Background(), db, info.KindVersion, &versions, bigquery.BigQuery())
	require.ErrorIs(t, err, errUnexpectedVersionConnection)
	require.EqualValues(t, 1, d.opens.Load())
}

func TestBigQueryFixedVersionHonorsCancellation(t *testing.T) {
	db, d := fixedVersionDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New().DetectProduct(ctx, db)
	require.ErrorIs(t, err, context.Canceled)
	var version string
	err = New().Info(ctx, db, info.KindVersion, &version, bigquery.BigQuery())
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, version)
	require.Zero(t, d.opens.Load())
}
