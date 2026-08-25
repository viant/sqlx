package io

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/viant/sqlx/metadata/sink"
)

// DetectColumns detects columns for the supplied SQL.
func DetectColumns(ctx context.Context, db *sql.DB, query string, args ...interface{}) ([]*sink.Column, error) {
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := TypesToColumns(columnTypes)
	result := make([]*sink.Column, 0, len(columns))
	for _, item := range columns {
		sinkColumn := &sink.Column{
			Name: item.Name(),
			Type: item.DatabaseTypeName(),
		}
		scanType := item.ScanType()
		sinkColumn.SetScanType(scanType)
		if scanType != nil {
			sinkColumn.TypeDefinition = scanType.String()
		}
		if sinkColumn.Type == "" {
			itemType := scanType
			if itemType != nil && itemType.Kind() == reflect.Pointer {
				itemType = itemType.Elem()
			}
			if itemType != nil {
				sinkColumn.Type = itemType.Name()
			}
			if sinkColumn.Type == "" {
				return nil, fmt.Errorf("unable discover column %v type", item.Name())
			}
		}
		if nullable, ok := item.Nullable(); ok && nullable {
			sinkColumn.Nullable = "1"
		}
		if length, ok := item.Length(); ok {
			sinkColumn.Length = &length
		}
		result = append(result, sinkColumn)
	}
	return result, nil
}
