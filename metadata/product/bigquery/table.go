package bigquery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	api "google.golang.org/api/bigquery/v2"
)

// tableMetadataHandler uses a single-table API rather than requiring permission
// to enumerate a dataset. Older drivers retain the registered SQL metadata path.
func tableMetadataHandler() info.Handler {
	return info.NewHandler(func(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var assign func([]sink.Column)
		switch dest := target.(type) {
		case *[]sink.Column:
			if dest != nil {
				assign = func(columns []sink.Column) { *dest = append(*dest, columns...) }
			}
		case *sink.Columns:
			if dest != nil {
				assign = func(columns []sink.Column) { *dest = append(*dest, columns...) }
			}
		}
		if assign == nil {
			return true, nil
		}
		var args *option.Args
		for _, opt := range options {
			if value, ok := opt.(*option.Args); ok {
				args = value
				break
			}
		}
		if args == nil || args.Size() != 3 {
			return true, nil
		}
		names, err := args.StringN(3)
		if err != nil {
			return false, err
		}
		for _, name := range names {
			if name == "" {
				return true, nil
			}
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Close()
		supported := false
		var table *api.Table
		err = conn.Raw(func(raw interface{}) error {
			capability, ok := raw.(interface {
				TableMetadata(context.Context, string, string, string) (*api.Table, error)
			})
			if !ok {
				return nil
			}
			supported = true
			table, err = capability.TableMetadata(ctx, names[0], names[1], names[2])
			return err
		})
		if err != nil {
			return false, err
		}
		if !supported {
			return true, nil
		}
		if table == nil || table.Schema == nil {
			return false, fmt.Errorf("bigquery: table metadata returned no schema")
		}
		// Match the registered SQL metadata contract: no default or key facts are
		// reported, and COLUMN_DEFAULT is the empty string.
		columns := make([]sink.Column, 0, len(table.Schema.Fields))
		for i, field := range table.Schema.Fields {
			nullable := "YES"
			if field.Mode == "REQUIRED" || field.Mode == "REPEATED" {
				nullable = "NO"
			}
			columns = append(columns, sink.Column{Catalog: names[0], Schema: names[1], Table: names[2], Name: field.Name, Position: i + 1, Type: tableFieldType(field), Nullable: nullable, Default: new(string)})
		}
		// Ingestion-time partition columns are hidden pseudo columns with no ordinal.
		if partition := table.TimePartitioning; partition != nil && partition.Field == "" {
			columns = append(columns, sink.Column{Catalog: names[0], Schema: names[1], Table: names[2], Name: "_PARTITIONTIME", Type: "TIMESTAMP", Nullable: "YES", Default: new(string)})
			if partition.Type == "DAY" {
				columns = append(columns, sink.Column{Catalog: names[0], Schema: names[1], Table: names[2], Name: "_PARTITIONDATE", Type: "DATE", Nullable: "YES", Default: new(string)})
			}
		}
		assign(columns)
		return false, nil
	})
}

func tableFieldType(field *api.TableFieldSchema) string {
	result := field.Type
	switch result {
	case "INTEGER":
		result = "INT64"
	case "FLOAT":
		result = "FLOAT64"
	case "BOOLEAN":
		result = "BOOL"
	case "RANGE":
		if field.RangeElementType != nil {
			result = "RANGE<" + field.RangeElementType.Type + ">"
		}
	case "RECORD", "STRUCT":
		fields := make([]string, len(field.Fields))
		for i, child := range field.Fields {
			fields[i] = "`" + strings.ReplaceAll(child.Name, "`", "\\`") + "` " + tableFieldType(child)
		}
		result = "STRUCT<" + strings.Join(fields, ", ") + ">"
	}
	if field.Mode == "REPEATED" {
		result = "ARRAY<" + result + ">"
	}
	return result
}
