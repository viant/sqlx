package io

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlx/metadata/sink"
)

// DetectColumns detects columns for the supplied SQL.
func DetectColumns(ctx context.Context, db *sql.DB, query string, args ...interface{}) ([]*sink.Column, error) {
	return (ColumnDetector{}).Detect(ctx, db, query, args...)
}

// ColumnDetector discovers SQL projection metadata without consuming rows.
type ColumnDetector struct {
	// UnmappedColumns identifies outputs that the caller will not scan into
	// fields. Missing database types are permitted only for these names, matched
	// case-insensitively. Their reported metadata is retained without inventing
	// a database or Go type; the caller owns their application types.
	UnmappedColumns []string
	// DeclaredColumns have application types owned by the caller. Missing
	// driver types are allowed without inventing a database or Go scan type.
	DeclaredColumns []string
	// ResolveTypes optionally supplies exact value types by result ordinal.
	// It receives metadata only, before rows are consumed. A non-pointer hint
	// is non-nullable; a pointer hint is nullable. Database type names remain
	// driver-owned. Declared application types remain outside this detector.
	ResolveTypes func([]Column) map[int]reflect.Type
}

// Detect discovers columns without sampling values. Missing driver types need
// an explicit declaration, unmapped policy, or a caller-supplied type.
func (d ColumnDetector) Detect(ctx context.Context, db *sql.DB, query string, args ...interface{}) ([]*sink.Column, error) {
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
	var hints map[int]reflect.Type
	if d.ResolveTypes != nil {
		hints = d.ResolveTypes(columns)
	}
	result := make([]*sink.Column, 0, len(columns))
	for i, item := range columns {
		sinkColumn := &sink.Column{
			Name: item.Name(),
			Type: item.DatabaseTypeName(),
		}
		scanType := item.ScanType()
		if hint := hints[i]; hint != nil {
			scanType = hint
		}
		sinkColumn.SetScanType(scanType)
		if scanType != nil {
			sinkColumn.TypeDefinition = scanType.String()
		}
		if sinkColumn.Type == "" && hints[i] == nil {
			itemType := scanType
			if itemType != nil && itemType.Kind() == reflect.Pointer {
				itemType = itemType.Elem()
			}
			if itemType != nil {
				sinkColumn.Type = itemType.Name()
			}
			if sinkColumn.Type == "" && !d.permitsUnknown(item.Name()) {
				return nil, fmt.Errorf("unable discover column %v type", item.Name())
			}
		}
		if nullable, ok := item.Nullable(); ok && nullable {
			sinkColumn.Nullable = "1"
		}
		if hint := hints[i]; hint != nil {
			sinkColumn.Nullable = ""
			if hint.Kind() == reflect.Pointer {
				sinkColumn.Nullable = "1"
			}
		}
		if length, ok := item.Length(); ok {
			sinkColumn.Length = &length
		}
		result = append(result, sinkColumn)
	}
	return result, nil
}

func (d ColumnDetector) permitsUnknown(name string) bool {
	for _, names := range [][]string{d.UnmappedColumns, d.DeclaredColumns} {
		for _, candidate := range names {
			if strings.EqualFold(candidate, name) {
				return true
			}
		}
	}
	return false
}
