package sequence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Lock acknowledges native nextval ownership: no allocator table, source DML,
// sequence restart, advisory lock or extra connection is required.
type Lock struct{}

func (*Lock) CanUse(...interface{}) bool { return true }
func (*Lock) Handle(ctx context.Context, _ *sql.DB, _ interface{}, _ ...interface{}) (bool, error) {
	return false, ctx.Err()
}

type Reserve struct{}

func (*Reserve) CanUse(...interface{}) bool { return true }
func (*Reserve) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Reservation)
	options := option.AsOptions(opts)
	count := options.RecordCount()
	if !ok || out == nil || count <= 0 {
		return false, fmt.Errorf("PostgreSQL reservation requires a Reservation target and positive count")
	}
	var q queryer = options.Tx()
	var conn *sql.Conn
	if options.Tx() == nil {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Close()
		q = conn
	}
	authority, err := (&Metadata{}).resolve(ctx, q, options)
	if err != nil {
		return false, err
	}
	// nextval is atomic and nontransactional. Concurrent calls and backend caches
	// may interleave: preserve each actual returned value, including descending
	// sequences, instead of inventing an arithmetic batch from its last value.
	rows, err := q.QueryContext(ctx, "SELECT pg_catalog.nextval($1::regclass) FROM pg_catalog.generate_series(1,$2::bigint)", authority.oid, count)
	if err != nil {
		return false, err
	}
	result := sink.Reservation{Sequence: authority.sequence, Values: make([]int64, 0, int(count))}
	for rows.Next() {
		var value int64
		if err = rows.Scan(&value); err != nil {
			rows.Close()
			return false, err
		}
		if value < authority.min || value > authority.max {
			rows.Close()
			return false, fmt.Errorf("PostgreSQL sequence value %d exceeds mapped column range [%d,%d]", value, authority.min, authority.max)
		}
		result.Values = append(result.Values, value)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return false, err
	}
	if closeErr != nil {
		return false, closeErr
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = result.Validate(int(count)); err != nil {
		return false, err
	}
	*out = result
	return false, nil
}

// RangeError rejects the old scalar range API before consuming sequence values.
type RangeError struct{}

func (*RangeError) CanUse(...interface{}) bool { return true }
func (*RangeError) Handle(context.Context, *sql.DB, interface{}, ...interface{}) (bool, error) {
	return false, fmt.Errorf("PostgreSQL nextval batches require ReserveSequence exact values; NextSequence cannot represent their gaps")
}
