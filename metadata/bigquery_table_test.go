package metadata_test

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	bqdriver "github.com/viant/bigquery"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	product "github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"google.golang.org/api/googleapi"
	apioption "google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBigQuerySingleTableMetadata(t *testing.T) {
	for _, code := range []int{200, 403, 404} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			tableCalls, sqlCalls := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method != "GET" || r.URL.Path != "/projects/source/datasets/data/tables/records" {
					sqlCalls++
					w.WriteHeader(403)
					_, _ = w.Write([]byte(`{"error":{"message":"dataset enumeration denied"}}`))
					return
				}
				tableCalls++
				require.Equal(t, "BASIC", r.URL.Query().Get("view"))
				w.WriteHeader(code)
				if code != 200 {
					_, _ = w.Write([]byte(`{"error":{"message":"table metadata denied"}}`))
					return
				}
				_, _ = w.Write([]byte(`{"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"title","type":"STRING"},{"name":"items","type":"RECORD","mode":"REPEATED","fields":[{"name":"active","type":"BOOLEAN"},{"name":"scores","type":"FLOAT","mode":"REPEATED"}]},{"name":"date_window","type":"RANGE","rangeElementType":{"type":"DATE"}},{"name":"timestamp_windows","type":"RANGE","mode":"REPEATED","rangeElementType":{"type":"TIMESTAMP"}},{"name":"record_window","type":"RECORD","fields":[{"name":"datetime_window","type":"RANGE","rangeElementType":{"type":"DATETIME"}}]}]},"timePartitioning":{"type":"DAY"}}`))
			}))
			defer server.Close()
			bqdriver.SetOptions(apioption.WithEndpoint(server.URL+"/"), apioption.WithoutAuthentication())
			defer bqdriver.SetOptions()
			db, err := sql.Open("bigquery", "bigquery://billing/default?jobProject=billing")
			require.NoError(t, err)
			defer db.Close()
			var columns []sink.Column
			err = metadata.New().Info(context.Background(), db, info.KindTable, &columns, product.BigQuery(), option.NewArgs("source", "data", "records"))
			if code != 200 {
				var apiErr *googleapi.Error
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, code, apiErr.Code)
				require.Empty(t, columns)
			} else {
				require.NoError(t, err)
				require.Len(t, columns, 8)
				require.Equal(t, "INT64", columns[0].Type)
				require.True(t, columns[0].IsNotNull())
				require.True(t, columns[1].IsNullable())
				require.Equal(t, "ARRAY<STRUCT<`active` BOOL, `scores` ARRAY<FLOAT64>>>", columns[2].Type)
				require.True(t, columns[2].IsNotNull())
				require.Equal(t, "RANGE<DATE>", columns[3].Type)
				require.Equal(t, "ARRAY<RANGE<TIMESTAMP>>", columns[4].Type)
				require.Equal(t, "STRUCT<`datetime_window` RANGE<DATETIME>>", columns[5].Type)
				require.Equal(t, "_PARTITIONTIME", columns[6].Name)
				require.Zero(t, columns[6].Position)
				require.Equal(t, "_PARTITIONDATE", columns[7].Name)
				for _, column := range columns {
					require.Equal(t, "source", column.Catalog)
					require.NotNil(t, column.Default)
					require.Empty(t, *column.Default)
					require.Empty(t, column.Key)
				}
			}
			require.Equal(t, 1, tableCalls)
			require.Zero(t, sqlCalls)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = metadata.New().Info(ctx, db, info.KindTable, &columns, product.BigQuery(), option.NewArgs("source", "data", "records"))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, tableCalls)
		})
	}
}
