package metadata_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	driver "github.com/viant/bigquery"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	product "github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"google.golang.org/api/googleapi"
	apioption "google.golang.org/api/option"
)

func TestBigQueryPaginatedTables(t *testing.T) {
	calls, status := 0, 200
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/projects/source/datasets/data/tables", r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("maxResults"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != 200 {
			_, _ = w.Write([]byte(`{"error":{"message":"denied"}}`))
			return
		}
		if r.URL.Query().Get("pageToken") == "next" {
			_, _ = w.Write([]byte(`{"tables":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"tables":[{"tableReference":{"projectId":"source","datasetId":"data","tableId":"records"},"type":"TABLE"}],"nextPageToken":"next"}`))
	}))
	defer server.Close()
	driver.SetOptions(apioption.WithEndpoint(server.URL+"/"), apioption.WithoutAuthentication())
	defer driver.SetOptions()
	db, err := sql.Open("bigquery", "bigquery://billing/default")
	require.NoError(t, err)
	defer db.Close()
	page := &option.MetadataPage{Limit: 7}
	var tables []sink.Table
	read := func(ctx context.Context) error {
		return metadata.New().Info(ctx, db, info.KindTables, &tables, product.BigQuery(), option.NewArgs("source", "data"), page)
	}
	require.NoError(t, read(context.Background()))
	require.Len(t, tables, 1)
	require.Equal(t, "records", tables[0].Name)
	require.Equal(t, "source", tables[0].Catalog)
	require.Nil(t, tables[0].SQL)
	require.True(t, page.Handled)
	require.Equal(t, "next", page.NextToken)
	require.Equal(t, 1, calls)
	page.Token = page.NextToken
	tables = nil
	require.NoError(t, read(context.Background()))
	require.Empty(t, tables)
	require.Empty(t, page.NextToken)
	require.Equal(t, 2, calls)
	status = 403
	err = read(context.Background())
	var cause *googleapi.Error
	require.ErrorAs(t, err, &cause)
	require.Equal(t, 403, cause.Code)
	require.False(t, page.Handled)
	require.Empty(t, tables)
	before := calls
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, read(ctx), context.Canceled)
	require.Equal(t, before, calls)
}
