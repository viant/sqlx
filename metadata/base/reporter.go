package base

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/base"
	"github.com/viant/sqlx/metadata"
	"reflect"
	"sort"
)

//Reporter represents a base reporter
type Reporter struct {
	queries map[QueryKind]Queries
	metadata.Dialect
}



func (r *Reporter) query(ctx context.Context, db *sql.DB, SQL string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.QueryContext(ctx, SQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %v, err: %w", SQL, err)
	}
	return rows, nil
}

func (r *Reporter) executeWithSingleArgCallback(ctx context.Context, db *sql.DB,  SQL string, fn func(name string), args ...interface{}) error {
	rows, err := r.query(ctx, db, SQL, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		fn(name)
		if ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

func (r *Reporter) executeSingle(ctx context.Context, db *sql.DB, SQL string, args ...interface{}) (string, error) {
	rows, err := r.query(ctx,db,  SQL, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", nil
	}
	var name string
	return name, rows.Scan(&name)
}



//Catalogs call fn with each vendor catalog
func (r *Reporter) Catalogs(ctx context.Context, db *sql.DB, fn func(name string)) error {
	queries, ok := r.queries[QueryKindCatalogs]
	if !ok || len(queries) == 0 {
		return base.NewUnsupported(fmt.Sprintf("%T", db.Driver()), "Catalogs")
	}
	query := queries.Match(r.Product.Info)
	return r.executeWithSingleArgCallback(ctx, db, query.SQL, fn)
}

//Schemas call fn with each vendor schema
func (r *Reporter) Schemas(ctx context.Context, db *sql.DB, catalog string, fn func(name string)) error {
	queries, ok := r.queries[QueryKindSchemas]
	if !ok || len(queries) == 0 {
		return base.NewUnsupported(fmt.Sprintf("%T", db.Driver()), "Schemas")
	}
	query := queries.Match(r.Product.Info)
	return r.executeWithSingleArgCallback(ctx, db, query.SQL, fn, catalog)
}

//Catalog returns default connection catalog
func (r *Reporter) Catalog(ctx context.Context, db *sql.DB,) (string, error) {
	queries, ok := r.queries[QueryKindCatalog]
	if !ok || len(queries) == 0 {
		return "", base.NewUnsupported(fmt.Sprintf("%T", db.Driver()), "Catalog")
	}
	query := queries.Match(r.Product.Info)
	return r.executeSingle(ctx, db, query.SQL)
}

//Schema returns default connection schema
func (r *Reporter) Schema(ctx context.Context, db *sql.DB,) (string, error) {
	queries, ok := r.queries[QueryKindSchema]
	if !ok || len(queries) == 0 {
		return "", base.NewUnsupported(fmt.Sprintf("%T", db.Driver()), "Schema")
	}
	query := queries.Match(r.Product.Info)
	return r.executeSingle(ctx, db, query.SQL)
}

//Tables call fn for each table in the specified catalog and schema
func (r *Reporter) Tables(ctx context.Context, db *sql.DB, catalog, schema string, fn func(table string)) error {
	queries, ok := r.queries[QueryKindTables]
	if !ok || len(queries) == 0 {
		return base.NewUnsupported(fmt.Sprintf("%T", db.Driver()), "Tables")
	}
	query := queries.Match(r.Product.Info)
	return r.executeWithSingleArgCallback(ctx, db, query.SQL, fn, catalog, schema)
}

//Table call fn for with table details
func (r *Reporter) Table(ctx context.Context, db *sql.DB, catalog, schema, table string, fn func(column sqlx.Column)) error {
	queries, ok := r.queries[QueryKindTable]
	if !ok || len(queries) == 0 {
		return base.NewUnsupported(fmt.Sprintf("%T",db.Driver()), "Table")
	}
	query := queries.Match(r.Product.Info)
	err := r.readColumns(ctx,db, catalog, schema, table, fn, query)
	if err == nil {
		return nil
	}
	if schema != "" {
		schema += "."
	}
	SQL := fmt.Sprintf("SELECT * FROM %s%s WHERE 1 = 0", schema, table)
	rows, err := r.query(ctx, db, SQL)
	if err == nil {
		return nil
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err == nil {
		return nil
	}
	for _, column := range columns {
		fn(column)
		if ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

func (r *Reporter) readColumns(ctx context.Context, db *sql.DB, catalog string, schema string, table string, fn func(column sqlx.Column), query Query) error {
	rows, err := r.query(ctx, db, query.SQL, catalog, schema, table)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var column = base.Column{}
		var scanType string
		if err := rows.Scan(&column.name,
			&column.databaseTypeName,
			&column.decimalPrecision,
			&column.length,
			&column.nullable,
			&scanType,
		); err != nil {
			return err
		}
		column.scanType = r.ScanType(scanType, column.databaseTypeName)
		fn(&column)
		if ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

func (r *Reporter) ScanType(scanType string, name string) reflect.Type {
	return nil
}


//NewReporter returns base report
func NewReporter(dialect metadata.Dialect, queries ...Query) Reporter {
	var queryMap = make(map[QueryKind]Queries)
	for i := range queries {
		queries[i].Init()
		if _, ok := queryMap[queries[i].Kind]; !ok {
			queryMap[queries[i].Kind] = Queries{}
		}
		queryMap[queries[i].Kind] = append(queryMap[queries[i].Kind], queries[i])
	}
	for k := range queryMap {
		sort.Sort(queryMap[k])
	}
	return Reporter{
		queries: queryMap,
		Dialect: dialect,
	}
}
