package bigquery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	api "google.golang.org/api/bigquery/v2"
)

// Explicit paging opts into the API. Existing unpaged metadata callers retain
// their complete SQL metadata contract, including DDL and creation timestamps.
func tablesMetadataHandler() info.Handler {
	return info.NewHandler(func(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (bool, error) {
		var page *option.MetadataPage
		var args *option.Args
		for _, opt := range options {
			switch value := opt.(type) {
			case *option.MetadataPage:
				page = value
			case *option.Args:
				args = value
			}
		}
		if page == nil {
			return true, nil
		}
		page.Handled, page.NextToken = false, ""
		if err := ctx.Err(); err != nil {
			return false, err
		}
		dest, ok := target.(*[]sink.Table)
		if !ok || dest == nil {
			return false, fmt.Errorf("bigquery: paginated tables require *[]sink.Table")
		}
		if args == nil || args.Size() != 2 || page.Limit <= 0 {
			return false, fmt.Errorf("bigquery: paginated tables require catalog, schema and positive limit")
		}
		names, err := args.StringN(2)
		if err != nil {
			return false, err
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Close()
		var result *api.TableList
		err = conn.Raw(func(raw interface{}) error {
			provider, ok := raw.(interface {
				ListTableMetadata(context.Context, string, string, int64, string) (*api.TableList, error)
			})
			if !ok {
				return fmt.Errorf("bigquery: driver does not support paginated table metadata")
			}
			result, err = provider.ListTableMetadata(ctx, names[0], names[1], page.Limit, page.Token)
			return err
		})
		if err != nil {
			return false, err
		}
		if result == nil {
			return false, fmt.Errorf("bigquery: missing table metadata page")
		}
		items := make([]sink.Table, 0, len(result.Tables))
		for _, table := range result.Tables {
			if table == nil || table.TableReference == nil {
				return false, fmt.Errorf("bigquery: table metadata page contains missing reference")
			}
			ref := table.TableReference
			kind := table.Type
			items = append(items, sink.Table{Catalog: ref.ProjectId, Schema: ref.DatasetId, Name: ref.TableId, Type: &kind})
		}
		*dest = append(*dest, items...)
		page.NextToken, page.Handled = result.NextPageToken, true
		return false, nil
	})
}
