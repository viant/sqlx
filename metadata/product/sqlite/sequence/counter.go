package sequence

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/viant/sqlx/metadata/sink"
)

// initializeCounter lets SQLite initialize its own AUTOINCREMENT entry for an
// empty table. The INSERT selects zero rows: no entity, trigger, default or FK
// is executed. Other numeric columns use the SQLX reservation table.
// This avoids parsing SQLite's stored DDL or manufacturing a counter for a table
// whose automatic inserts would ignore it.
func (*Next) initializeCounter(ctx context.Context, tx *sql.Tx, identity sink.Sequence) error {
	rows, err := tx.QueryContext(ctx, "SELECT name, type FROM pragma_table_info(?, ?) WHERE pk > 0", identity.Name, identity.Schema)
	if err != nil {
		return err
	}
	var name, kind string
	count := 0
	for rows.Next() {
		if err = rows.Scan(&name, &kind); err != nil {
			rows.Close()
			return err
		}
		count++
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if count != 1 || !strings.EqualFold(kind, "INTEGER") {
		return nil
	}
	table := quoteIdentifier(identity.Schema) + "." + quoteIdentifier(identity.Name)
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+table+" ("+quoteIdentifier(name)+") SELECT NULL WHERE 0"); err != nil {
		return err
	}
	return nil
}

// engineCounter reads SQLite's AUTOINCREMENT authority when present. Ordinary
// and composite keys have no engine counter; their reservation lives in SQLX.
func (n *Next) engineCounter(ctx context.Context, tx *sql.Tx, identity sink.Sequence) (int64, bool, error) {
	ledger := quoteIdentifier(identity.Schema) + ".sqlite_sequence"
	read := func() (int64, bool, error) {
		rows, err := tx.QueryContext(ctx, "SELECT seq FROM "+ledger+" WHERE name=?", identity.Name)
		if err != nil {
			if err.Error() == "no such table: "+identity.Schema+".sqlite_sequence" {
				return 0, false, nil
			}
			return 0, false, err
		}
		defer rows.Close()
		var value int64
		count := 0
		for rows.Next() {
			if err = rows.Scan(&value); err != nil {
				return 0, false, err
			}
			count++
		}
		if err = rows.Err(); err != nil {
			return 0, false, err
		}
		if count > 1 {
			return 0, false, errors.New("SQLite sequence has duplicate engine counter rows")
		}
		return value, count == 1, nil
	}
	value, exists, err := read()
	if exists || err != nil {
		return value, exists, err
	}
	if err = n.initializeCounter(ctx, tx, identity); err != nil {
		return 0, false, err
	}
	return read()
}
