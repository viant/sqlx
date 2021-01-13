package metadata

import (
	"context"
	"database/sql"
)

type Meta interface {

	Info(ctx context.Context, db *sql.DB, kind Kind, sink Sink, options ... Option) error



	////Tables call fn for each table in the specified catalog and schema
	//Tables(ctx context.Context, db *sql.DB, schema *Schema, fn func(table string)) error
	//
	////Table call fn for with table details
	//Table(ctx context.Context, db *sql.DB, schema *Schema, table string, fn func(column Column)) error
	//

	//Views(ctx context.Context, catalog, schema string, fn func(name string) (toContinue bool)) error
	//View(ctx context.Context, catalog, schema, fn func(columns []*sql.ColumnType, props Properties)) error
	//PrimaryKeys(ctx context.Context, catalog, schema, table string, fn func(columns []string)) error
	//ExportedKeys(ctx context.Context, catalog, schema, table string, fn func(pkTable string, columns []string)) error
	//Indexes(ctx context.Context, catalog, schema string, fn func(name string) (toContinue bool)) error
	//Sequences(ctx context.Context, catalog, schema string, fn func(name string) bool) error
	//Sequence(ctx context.Context, catalog, schema, name string) (int64, error)
}

