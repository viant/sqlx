package sequence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// ReservationLock defers row locking until native column identity is known.
// The InnoDB reservation uses a current locking read, including under RR.
type ReservationLock struct{}

func (*ReservationLock) CanUse(...interface{}) bool { return true }
func (*ReservationLock) Handle(ctx context.Context, _ *sql.DB, _ interface{}, _ ...interface{}) (bool, error) {
	return false, ctx.Err()
}

type Reserve struct{ rangeOnly bool }

func (*Reserve) CanUse(...interface{}) bool { return true }
func (n *Reserve) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Reservation)
	options := option.AsOptions(opts)
	count := options.RecordCount()
	if !ok || out == nil || count <= 0 {
		return false, fmt.Errorf("MySQL reservation requires a Reservation target and positive count")
	}
	tx := options.Tx()
	owned := tx == nil
	if owned {
		var err error
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
	}
	authority, err := (&ReservationMetadata{}).resolve(ctx, tx, options)
	if err != nil {
		return false, err
	}
	if err = (&Store{}).check(ctx, tx); err != nil {
		return false, err
	}
	args := []any{authority.sequence.Schema, authority.table, authority.column}
	// ON DUPLICATE KEY UPDATE takes the same row lock for first and later use.
	// No application INSERT, ALTER, FK toggle, advisory lock or second tx occurs.
	if _, err = tx.ExecContext(ctx, "INSERT INTO "+reservationTable+" (table_schema,table_name,column_name,value) VALUES (?,?,?,0) ON DUPLICATE KEY UPDATE value=value", args...); err != nil {
		return false, err
	}
	var current int64
	where := " WHERE table_schema=? AND table_name=? AND column_name=?"
	if err = tx.QueryRowContext(ctx, "SELECT value FROM "+reservationTable+where+" FOR UPDATE", args...).Scan(&current); err != nil {
		return false, err
	}
	if current < 0 {
		return false, fmt.Errorf("MySQL native allocator counter is negative")
	}
	// This is a current read, not a repeatable-read snapshot MAX. It observes
	// earlier caller writes and committed source IDs without modifying a row.
	var maximum int64
	source := quoteName(authority.sequence.Schema) + "." + quoteName(authority.table)
	column := quoteName(authority.column)
	err = tx.QueryRowContext(ctx, "SELECT "+column+" FROM "+source+" ORDER BY "+column+" DESC LIMIT 1 FOR UPDATE").Scan(&maximum)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if maximum > current {
		current = maximum
	}
	autoNext, err := (&ReservationMetadata{}).currentAutoValue(ctx, tx, source, authority.mode)
	if err != nil {
		return false, err
	}
	if autoNext > uint64(math.MaxInt64) {
		return false, fmt.Errorf("MySQL AUTO_INCREMENT exceeds the int64 reservation contract")
	}
	if lower := int64(autoNext) - 1; lower > current {
		current = lower
	}
	start, step, limit := authority.sequence.StartValue, authority.sequence.IncrementBy, authority.sequence.MaxValue
	first := start
	if current >= start {
		steps := (current-start)/step + 1
		if steps > (math.MaxInt64-start)/step {
			return false, fmt.Errorf("MySQL sequence reservation overflows int64")
		}
		first = start + steps*step
	}
	if first > limit || (count-1) > (limit-first)/step {
		return false, fmt.Errorf("MySQL reservation exceeds %s column maximum %d", authority.sequence.DataType, limit)
	}
	last := first + (count-1)*step
	if n.rangeOnly && last > math.MaxInt64-step {
		return false, fmt.Errorf("MySQL range endpoint exceeds int64; use ReserveSequence exact values")
	}
	updateArgs := append([]any{last}, args...)
	update, err := tx.ExecContext(ctx, "UPDATE "+reservationTable+" SET value=?"+where, updateArgs...)
	if err != nil {
		return false, err
	}
	affected, err := update.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, fmt.Errorf("MySQL reservation updated %d counters, expected one", affected)
	}
	var stored int64
	if err = tx.QueryRowContext(ctx, "SELECT value FROM "+reservationTable+where+" FOR UPDATE", args...).Scan(&stored); err != nil {
		return false, err
	}
	if stored != last {
		return false, fmt.Errorf("MySQL reservation counter did not persist")
	}
	result := sink.Reservation{Sequence: authority.sequence, Values: make([]int64, int(count))}
	for i := range result.Values {
		result.Values[i] = first + int64(i)*step
	}
	if err = result.Validate(int(count)); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if owned {
		if err = tx.Commit(); err != nil {
			return false, err
		}
	}
	*out = result
	return false, nil
}
