package io

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/sink"
	"reflect"
)

// DetectColumns detect columns for supplied SQL
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
	var tableColumns []*sink.Column
	if rows != nil {
		columnsTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		if len(columnsTypes) != 0 {
			columns := TypesToColumns(columnsTypes)
			for _, item := range columns {
				sinkColumn := &sink.Column{
					Name: item.Name(),
					Type: item.DatabaseTypeName(),
				}
				sinkColumn.SetScanType(item.ScanType())
				if rType := item.ScanType(); rType != nil {
					sinkColumn.TypeDefinition = rType.String()
				}
				if sinkColumn.Type == "" {
					if itemType := item.ScanType(); itemType != nil {
						if itemType.Kind() == reflect.Pointer {
							itemType = itemType.Elem()
						}
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
				tableColumns = append(tableColumns, sinkColumn)
			}
		}
	}
	return tableColumns, nil
}
