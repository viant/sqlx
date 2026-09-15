package sequence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/option"
)

// Lock acquires database write intent before sequence identity reads. It uses
// the supplied transaction exclusively and never completes it. SQLite permits
// only one writer; a concurrent invocation waits here until that owner finishes.
type Lock struct{}

func (*Lock) CanUse(...interface{}) bool { return true }

func (*Lock) Handle(ctx context.Context, db *sql.DB, _ interface{}, opts ...interface{}) (bool, error) {
	options := option.AsOptions(opts)
	tx := options.Tx()
	if tx == nil {
		return false, ctx.Err()
	}
	table := options.SequenceTable()
	if table == "" {
		identity, err := (&Metadata{}).identity(ctx, nil, options)
		if err != nil {
			return false, err
		}
		table = quoteIdentifier(identity.Name)
		if identity.Schema != "" {
			table = quoteIdentifier(identity.Schema) + "." + table
		}
	}
	parts, err := sqlparser.TableIdentifierParts(table)
	if err != nil {
		return false, err
	}
	if len(parts) == 0 || len(parts) > 2 {
		return false, fmt.Errorf("SQLite sequence requires a table or schema.table")
	}
	// pragma_database_list reads connection configuration, not table contents.
	// Lock attachments in file order, independent of attachment aliases/order.
	rows, err := tx.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return false, err
	}
	type database struct{ name, file string }
	var databases []database
	for rows.Next() {
		var pos int
		var item database
		if err = rows.Scan(&pos, &item.name, &item.file); err != nil {
			rows.Close()
			return false, err
		}
		if len(parts) == 1 || strings.EqualFold(parts[0], item.name) {
			databases = append(databases, item)
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return false, err
	}
	if closeErr != nil {
		return false, closeErr
	}
	if len(databases) == 0 {
		return false, fmt.Errorf("SQLite sequence schema was not found")
	}
	sort.Slice(databases, func(i, j int) bool { return databases[i].file < databases[j].file })

	for _, item := range databases {
		ledger := quoteIdentifier(item.name) + "." + quoteIdentifier(reservationTable)
		lockSQL := "UPDATE " + ledger + " SET value=value WHERE 0"
		if _, err = tx.ExecContext(ctx, lockSQL); err == nil {
			continue
		}
		if err.Error() != "no such table: "+item.name+"."+reservationTable {
			return false, fmt.Errorf("acquire SQLite sequence write intent: %w", err)
		}
		// UPDATE of a missing table fails at prepare time without establishing a
		// read snapshot. Do not use CREATE IF NOT EXISTS: its no-op path reads
		// before writing, reintroducing the deferred-transaction upgrade race.
		_, createErr := tx.ExecContext(ctx, "CREATE TABLE "+ledger+" (table_name TEXT PRIMARY KEY, value INTEGER NOT NULL CHECK(value >= 0))")
		// A competing creator can finish while CREATE waits. Retry the actual
		// write operation; this both validates the counter column and takes intent.
		if _, err = tx.ExecContext(ctx, lockSQL); err != nil {
			return false, errors.Join(fmt.Errorf("acquire SQLite sequence write intent: %w", err), createErr)
		}
	}
	return false, nil
}

const reservationTable = "sqlx_sequence_reservations"

func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
