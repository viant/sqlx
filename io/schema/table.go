// Package schema owns non-destructive table provisioning through SQLX metadata.
package schema

import (
	"fmt"
	"strings"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
)

// Table describes mapped columns and a parsed table destination. No raw DDL,
// default expressions, automatic IDs, migrations or destructive operations.
type Table struct {
	Name    string
	Dataset string
	Columns []io.Column
}

// CreateSQL renders a create-if-absent statement using the native dialect.
func (t Table) CreateSQL(dialect *info.Dialect) (string, error) {
	name, err := dialect.TableIdentifier(t.Name, t.Dataset)
	if err != nil {
		return "", err
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("table columns are required")
	}
	definitions := make([]string, 0, len(t.Columns)+1)
	var keys []string
	seen := map[string]bool{}
	for _, col := range t.Columns {
		if col == nil {
			return "", fmt.Errorf("nil table column")
		}
		column, err := dialect.MappedColumnIdentifier(col.Name())
		if err != nil {
			return "", err
		}
		key := strings.ToLower(column)
		if seen[key] {
			return "", fmt.Errorf("duplicate mapped column %s", col.Name())
		}
		seen[key] = true
		tag := col.Tag()
		if tag != nil && tag.Autoincrement {
			return "", fmt.Errorf("automatic IDs are not supported by table provisioning")
		}
		primary := tag != nil && tag.PrimaryKey
		length, _ := col.Length()
		storage, err := dialect.StorageType(col.ScanType(), length)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", col.Name(), err)
		}
		if primary && strings.EqualFold(dialect.Name, "mysql") && storage == "TEXT" {
			return "", fmt.Errorf("MySQL string primary key %s requires a bounded length", col.Name())
		}
		definition := column + " " + storage
		nullable, known := col.Nullable()
		if !known {
			return "", fmt.Errorf("column %s nullability is required", col.Name())
		}
		if !nullable || primary {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
		if primary {
			keys = append(keys, column)
		}
	}
	if len(keys) > 0 && dialect.SupportsPrimaryKey() {
		definitions = append(definitions, "PRIMARY KEY ("+strings.Join(keys, ", ")+")")
	}
	return "CREATE TABLE IF NOT EXISTS " + name + " (\n " + strings.Join(definitions, ",\n ") + "\n)", nil
}
