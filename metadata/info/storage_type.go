package info

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// StorageType returns the scalar DDL type for a mapped Go column. Unsupported
// products and composite types fail closed; this is not an arbitrary SQL type
// expression parser. A positive string length requests bounded storage.
func (d *Dialect) StorageType(t reflect.Type, length int64) (string, error) {
	if d == nil || t == nil {
		return "", fmt.Errorf("storage dialect and type are required")
	}
	if length < 0 || length > 65535 {
		return "", fmt.Errorf("unsupported string length %d", length)
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var text, integer, floating, boolean, timestamp string
	switch strings.ToLower(d.Name) {
	case "sqlite":
		text, integer, floating, boolean, timestamp = "TEXT", "INTEGER", "REAL", "BOOLEAN", "DATETIME"
	case "mysql":
		text, integer, floating, boolean, timestamp = "TEXT", "BIGINT", "DOUBLE", "BOOLEAN", "DATETIME(6)"
		if length > 0 {
			text = fmt.Sprintf("VARCHAR(%d)", length)
		}
	case "postgresql":
		text, integer, floating, boolean, timestamp = "TEXT", "BIGINT", "DOUBLE PRECISION", "BOOLEAN", "TIMESTAMP WITH TIME ZONE"
	case "bigquery":
		text, integer, floating, boolean, timestamp = "STRING", "INT64", "FLOAT64", "BOOL", "TIMESTAMP"
	default:
		return "", fmt.Errorf("table creation unsupported for %q", d.Name)
	}
	if t == reflect.TypeOf(time.Time{}) {
		return timestamp, nil
	}
	switch t.Kind() {
	case reflect.String:
		return text, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return integer, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if t.Bits() < 64 {
			return integer, nil
		}
		// Wide unsigned values exceed signed BIGINT. SQLite has no exact
		// numeric storage class covering their full range.
		switch strings.ToLower(d.Name) {
		case "mysql":
			return "BIGINT UNSIGNED", nil
		case "postgresql", "bigquery":
			return "NUMERIC(20,0)", nil
		default:
			return "", fmt.Errorf("unsigned 64-bit storage is unsupported for %s", d.Name)
		}
	case reflect.Float32, reflect.Float64:
		return floating, nil
	case reflect.Bool:
		return boolean, nil
	}
	return "", fmt.Errorf("unsupported scalar storage type %v for %s", t, d.Name)
}

// SupportsPrimaryKey reports whether this schema renderer emits an enforced key.
// BigQuery does not enforce primary keys.
func (d *Dialect) SupportsPrimaryKey() bool {
	return d != nil && !strings.EqualFold(d.Name, "bigquery")
}
