package sequence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Next reserves a durable SQLite range without inserting entity rows. A supplied
// transaction retains completion ownership; its reservation is pending until
// that owner commits. MAX allocation remains a separate read-only strategy.
type Next struct{}

func (n *Next) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	options := option.AsOptions(opts)
	result, ok := target.(*sink.Sequence)
	count := options.RecordCount()
	if !ok || result == nil || count <= 0 || count >= math.MaxInt64 {
		return false, fmt.Errorf("sequence reservation requires a Sequence target and positive bounded count")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tx := options.Tx()
	owned := tx == nil
	var queryer sequenceQueryer = tx
	var connection *sql.Conn
	if owned {
		var err error
		connection, err = db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer connection.Close()
		queryer = connection
	}
	if !owned {
		if _, err := (&Lock{}).Handle(ctx, db, nil, opts...); err != nil {
			return false, err
		}
	}
	identity, err := n.identity(ctx, queryer, options)
	if err != nil {
		return false, err
	}
	if owned {
		// Resolve before BeginTx so lock acquisition is the first transaction
		// statement, not an unsafe SQLite read-to-write snapshot upgrade.
		tx, err = connection.BeginTx(ctx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
	}

	lockOptions := []interface{}{tx, option.SequenceTable(quoteIdentifier(identity.Schema) + "." + quoteIdentifier(identity.Name))}
	if _, err := (&Lock{}).Handle(ctx, db, nil, lockOptions...); err != nil {
		return false, err
	}
	verified, err := n.identity(ctx, tx, options)
	if err != nil {
		return false, err
	}
	if verified.Catalog != identity.Catalog || verified.Schema != identity.Schema || verified.Name != identity.Name {
		return false, fmt.Errorf("SQLite sequence identity changed before reservation")
	}
	ledger := quoteIdentifier(identity.Schema) + "." + quoteIdentifier(reservationTable)
	var current int64
	err = tx.QueryRowContext(ctx, "SELECT value FROM "+ledger+" WHERE table_name=?", identity.Name).Scan(&current)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return false, err
	}
	engineValue, hasEngineCounter, err := n.engineCounter(ctx, tx, identity)
	if err != nil {
		return false, err
	}
	if engineValue > current {
		current = engineValue
	}
	if !hasEngineCounter && options.MaxIDSQLBuilder() == nil {
		return false, fmt.Errorf("SQLite numeric reservation requires the insert mapper's maximum-value query")
	}
	if builder := options.MaxIDSQLBuilder(); builder != nil {
		query := builder()
		if query == nil {
			return false, fmt.Errorf("sequence MAX query is required")
		}
		var maximum int64
		if err := tx.QueryRowContext(ctx, query.Query, query.Args...).Scan(&maximum); err != nil {
			return false, err
		}
		if maximum > current {
			current = maximum
		}
	}
	if current < 0 {
		return false, fmt.Errorf("SQLite sequence counter is negative")
	}
	if current > math.MaxInt64-count-1 {
		return false, fmt.Errorf("sequence range overflows int64")
	}
	last := current + count
	query := "UPDATE " + ledger + " SET value=? WHERE table_name=?"
	if missing {
		query = "INSERT INTO " + ledger + " (value,table_name) VALUES (?,?)"
	}
	written, err := tx.ExecContext(ctx, query, last, identity.Name)
	if err != nil {
		return false, err
	}
	affected, err := written.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, fmt.Errorf("SQLite sequence reservation affected %d rows, expected 1", affected)
	}
	var stored int64
	if err := tx.QueryRowContext(ctx, "SELECT value FROM "+ledger+" WHERE table_name=?", identity.Name).Scan(&stored); err != nil {
		return false, err
	}
	if stored != last {
		return false, fmt.Errorf("SQLite sequence reservation did not persist its counter")
	}

	if hasEngineCounter {
		engineLedger := quoteIdentifier(identity.Schema) + ".sqlite_sequence"
		updated, err := tx.ExecContext(ctx, "UPDATE "+engineLedger+" SET seq=? WHERE name=?", last, identity.Name)
		if err != nil {
			return false, err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return false, err
		}
		if count != 1 {
			return false, fmt.Errorf("SQLite engine counter update affected %d rows", count)
		}
		var stored int64
		if err := tx.QueryRowContext(ctx, "SELECT seq FROM "+engineLedger+" WHERE name=?", identity.Name).Scan(&stored); err != nil {
			return false, err
		}
		if stored != last {
			return false, fmt.Errorf("SQLite engine counter update did not persist")
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if owned {
		if err := tx.Commit(); err != nil {
			// database/sql marks Tx done even when older SQLite drivers leave
			// the physical transaction open after a failed COMMIT.
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rollbackErr := tx.Rollback()
			if errors.Is(rollbackErr, sql.ErrTxDone) {
				_, rollbackErr = connection.ExecContext(cleanup, "ROLLBACK")
			}
			if rollbackErr != nil {
				_ = connection.Raw(func(interface{}) error { return driver.ErrBadConn })
			}
			return false, errors.Join(fmt.Errorf("commit SQLite sequence reservation: %w", err), rollbackErr)
		}
	}
	identity.Value, identity.MaxValue = last+1, math.MaxInt64
	*result = identity
	return false, nil
}

func (n *Next) CanUse(opts ...interface{}) bool { return true }

func (n *Next) identity(ctx context.Context, queryer sequenceQueryer, options option.Options) (sink.Sequence, error) {
	metadata := &Metadata{}
	identity, err := metadata.identity(ctx, queryer, options)
	if err != nil {
		return identity, err
	}
	if options.SequenceTable() == "" {
		// Direct metadata callers pass the table as the sequence name.
		if err := metadata.resolveIdentity(ctx, queryer, &identity, identity.Schema); err != nil {
			return identity, err
		}
	}
	if identity.Name == reservationTable {
		return identity, fmt.Errorf("SQLite allocator metadata cannot allocate itself")
	}
	if identity.Name == "" || identity.Schema == "" {
		return identity, fmt.Errorf("native SQLite reservation identity is unresolved")
	}
	return identity, nil
}
