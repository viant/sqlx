package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"math"
)

type ReservationRange struct{}

func (*ReservationRange) CanUse(opts ...interface{}) bool {
	return option.AsOptions(opts).PresetIDStrategy() == dialect.PresetIDWithReservation
}
func (*ReservationRange) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Sequence)
	if !ok || out == nil {
		return false, fmt.Errorf("MySQL range requires a Sequence target")
	}
	values := &sink.Reservation{}
	if _, err := (&Reserve{rangeOnly: true}).Handle(ctx, db, values, opts...); err != nil {
		return false, err
	}
	last := values.Values[len(values.Values)-1]
	if last > math.MaxInt64-values.Sequence.IncrementBy {
		return false, fmt.Errorf("MySQL range endpoint exceeds int64; use ReserveSequence exact values")
	}
	seq := values.Sequence
	seq.Value = last + seq.IncrementBy
	*out = seq
	return false, nil
}

// StrategyError prevents a placeholder query from pretending to allocate IDs.
type StrategyError struct{}

func (*StrategyError) CanUse(...interface{}) bool { return true }
func (*StrategyError) Handle(_ context.Context, _ *sql.DB, _ interface{}, opts ...interface{}) (bool, error) {
	return false, fmt.Errorf("MySQL sequence strategy %q has no allocation handler; use ReserveSequence", option.AsOptions(opts).PresetIDStrategy())
}
