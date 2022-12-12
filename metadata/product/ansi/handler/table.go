package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"strconv"
)

type table struct{}

// Handle default implementation Handler's Handle function
func (h *table) Handle(ctx context.Context, db *sql.DB, target interface{}, iopts ...interface{}) (doNext bool, err error) {
	destPtr, ok := target.(*[]*sink.Column)
	var dest *[]sink.Column
	if !ok {
		dest, ok = target.(*[]sink.Column)
	}
	if !ok {
		return false, fmt.Errorf("invalid sink expected: %T, but had: %T", destPtr, target)
	}
	options := option.AsOptions(iopts)
	args := options.Args()
	if args.Size() < 3 {
		return false, fmt.Errorf("inalid arguments count, expected: 3 but had: %v", args.Size())
	}
	params, _ := args.StringN(3)
	tableName := params[2]
	if tableName == "" {
		return false, fmt.Errorf("table name was empty")
	}
	SQL := "SELECT * FROM " + tableName + " LIMIT 10"
	rows, err := db.QueryContext(ctx, SQL)
	if err != nil {
		return false, err
	}
	columnInfo, err := rows.Columns()
	if err != nil {
		return false, err
	}
	columnTypeInfo, err := rows.ColumnTypes()
	if err != nil {
		return false, err
	}
	for i, column := range columnInfo {
		typeInfo := columnTypeInfo[i]
		col := sink.Column{Name: column, Table: tableName, Type: typeInfo.DatabaseTypeName()}
		if nullable, ok := typeInfo.Nullable(); ok {
			col.Nullable = strconv.FormatBool(nullable)
		}
		if dest != nil {
			*dest = append(*dest, col)
			continue
		}
		*destPtr = append(*destPtr, &col)
	}
	return false, nil
}

// CanUse default implementation Handler's CanUse function
func (h *table) CanUse(options ...interface{}) bool {
	return true
}
