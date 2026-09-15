package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Reserve adapts the existing durable SQLite range owner to exact values.
type Reserve struct{}

func (*Reserve) CanUse(...interface{}) bool { return true }
func (*Reserve) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Reservation)
	if !ok || out == nil {
		return false, fmt.Errorf("SQLite reservation requires a Reservation target")
	}
	seq := &sink.Sequence{}
	if _, err := (&Next{}).Handle(ctx, db, seq, opts...); err != nil {
		return false, err
	}
	result, err := seq.Reservation(int(option.AsOptions(opts).RecordCount()))
	if err != nil {
		return false, err
	}
	*out = *result
	return false, nil
}
