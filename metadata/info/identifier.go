package info

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/viant/sqlparser"
)

// TableIdentifier validates and renders a table path and optional namespace.
// Names are parsed by SQLParser; quoted dots retain their product semantics.
func (d *Dialect) TableIdentifier(table, dataset string) (string, error) {
	if d == nil {
		return "", fmt.Errorf("table dialect is required")
	}
	parser := sqlparser.TableIdentifierParser{Product: d.Name}
	parts, err := parser.Parts(table)
	if err != nil {
		return "", fmt.Errorf("table identifier: %w", err)
	}
	if dataset != "" {
		if len(parts) != 1 {
			return "", fmt.Errorf("dataset cannot be combined with an already qualified table")
		}
		prefix, err := parser.Parts(dataset)
		if err != nil {
			return "", fmt.Errorf("dataset identifier: %w", err)
		}
		parts = append(prefix, parts...)
	}
	maxParts := 2
	switch strings.ToLower(d.Name) {
	case "bigquery":
		maxParts = 3
	case "mysql", "postgresql", "sqlite", "ansi":
	default:
		return "", fmt.Errorf("table identifiers unsupported for %q", d.Name)
	}
	if len(parts) > maxParts {
		return "", fmt.Errorf("too many table qualifiers for %s", d.Name)
	}
	if strings.EqualFold(d.Name, "bigquery") {
		parts = []string{strings.Join(parts, ".")}
	}
	for i, part := range parts {
		parts[i], err = d.QuoteIdentifier(part)
		if err != nil {
			return "", err
		}
	}
	return strings.Join(parts, "."), nil
}

// QuoteIdentifier renders one decoded identifier, never an SQL expression.
func (d *Dialect) QuoteIdentifier(name string) (string, error) {
	if d == nil {
		return "", fmt.Errorf("identifier dialect is required")
	}
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("invalid empty, NUL or non-UTF8 identifier")
	}
	switch strings.ToLower(d.Name) {
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`", nil
	case "bigquery":
		// SQLParser accepts whole-path backticks but not escaped backticks. Reject
		// backslashes too: GoogleSQL uses backslash escapes inside quoted names.
		if strings.ContainsAny(name, "`\\\r\n") {
			return "", fmt.Errorf("unsupported BigQuery identifier escape")
		}
		return "`" + name + "`", nil
	case "postgresql", "sqlite", "ansi":
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`, nil
	}
	return "", fmt.Errorf("identifier quoting unsupported for %q", d.Name)
}

// MappedColumnIdentifier quotes a SQLX mapped column name. SQLX DML uses
// unquoted column names, so PostgreSQL's normal lowercase folding must match.
func (d *Dialect) MappedColumnIdentifier(name string) (string, error) {
	if d != nil && strings.EqualFold(d.Name, "postgresql") {
		name = strings.ToLower(name)
	}
	return d.QuoteIdentifier(name)
}
