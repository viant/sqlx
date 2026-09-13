package sequence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/sink"
)

type sequenceQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// resolveIdentity delegates identifier syntax to SQLparser and name matching
// to SQLite itself. Actual stored schema/table names unify quoting/case aliases;
// unqualified lookup follows SQLite's temp/main/attachment search order.
func (n *Max) resolveIdentity(ctx context.Context, queryer sequenceQueryer, sequence *sink.Sequence) error {
	if sequence.Name == "" {
		return nil
	}
	parts, err := sqlparser.TableIdentifierParts(sequence.Name)
	if err != nil {
		return err
	}
	if len(parts) > 2 {
		return fmt.Errorf("SQLite table identity permits schema and table only")
	}
	query := "SELECT name FROM pragma_database_list"
	var args []any
	if len(parts) == 2 {
		query += " WHERE name = ? COLLATE NOCASE"
		args = append(args, parts[0])
	}
	query += " ORDER BY CASE WHEN name = 'temp' THEN -1 ELSE seq END"
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var schemas []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		schemas = append(schemas, name)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, schema := range schemas {
		// Schema values come from SQLite metadata, but still need SQL quoting.
		query = `SELECT name FROM "` + strings.ReplaceAll(schema, `"`, `""`) + `".sqlite_master WHERE name = ? COLLATE NOCASE AND type IN ('table','view')`
		var name string
		err = queryer.QueryRowContext(ctx, query, parts[len(parts)-1]).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		sequence.Schema, sequence.Name = schema, name
		return nil
	}
	return fmt.Errorf("SQLite sequence table %q was not found", sequence.Name)
}
