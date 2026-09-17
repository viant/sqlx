package insert

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// ReserveSequence allocates exact numeric values using the selected native
// strategy. Transient strategies may execute source DML and roll it back.
// A supplied transaction retains ownership. Products choose their native
// reservation semantics; no contiguous range is inferred from sequence outputs.
func (s *Service) ReserveSequence(ctx context.Context, records any, count int, options ...option.Option) (result *sink.Reservation, err error) {
	options = append(append([]option.Option(nil), options...), s.options...)
	defer func() {
		if err != nil {
			err = errors.Join(err, ctx.Err())
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, fmt.Errorf("sequence reservation requires a positive count")
	}
	at, size, err := io.Values(records)
	if err != nil {
		return nil, err
	}
	if size == 0 || at(0) == nil {
		return nil, fmt.Errorf("sequence reservation requires a typed record")
	}
	if err = s.prepareSequenceReservation(ctx, options); err != nil {
		return nil, err
	}
	sess, err := s.NewSession(ctx, at(0), option.Options(options).Db(), option.Options(options).BatchSize(), options...)
	if err != nil {
		return nil, err
	}
	numeric, err := sess.sequenceUpdater(options)
	if err != nil {
		return nil, err
	}
	strategy := sess.reservationStrategy(options)
	options = append([]option.Option{strategy}, options...)
	switch strategy {
	case dialect.PresetIDWithTransientTransaction, dialect.PresetIDWithUDFSequence:
		result, err := s.reserveFromNextSequence(ctx, records, count, options)
		if err != nil {
			return nil, err
		}
		if err = numeric.validateReservationValues(result); err != nil {
			return nil, err
		}
		return result, nil
	case "", dialect.PresetIDStrategyUndefined, dialect.PresetIDWithReservation:
		return numeric.reserveSequence(ctx, sess, count, options)
	default:
		return nil, fmt.Errorf("strategy %q does not provide allocated sequence values", strategy)
	}
}

func (n *numericSequencer) reserveSequence(ctx context.Context, sess *session, count int, options []option.Option) (*sink.Reservation, error) {
	_, err := n.reservationValueType()
	if err != nil {
		return nil, err
	}
	opts := []option.Option{option.SequenceTable(sess.TableName), option.SequenceColumn(n.column.Name()), option.RecordCount(count), option.NewArgs(sess.info.Catalog, sess.info.Schema, n.getSequenceName(sess)), n.maxIDSQLBuilder(sess)}
	if sess.Transaction != nil && sess.Transaction.Tx != nil {
		opts = append(opts, sess.Transaction.Tx)
	}
	opts = append(opts, n.options...)
	opts = append(opts, options...)
	opts = append(opts, sess.Dialect)
	result := &sink.Reservation{}
	if err := metadata.New().Info(ctx, sess.db, info.KindSequenceReservation, result, opts...); err != nil {
		return nil, err
	}
	if err := result.Validate(count); err != nil {
		return nil, err
	}
	if err := n.validateReservationValues(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (n *numericSequencer) validateReservationValues(result *sink.Reservation) error {
	valueType, err := n.reservationValueType()
	if err != nil {
		return err
	}
	target := reflect.New(valueType).Elem()
	for _, value := range result.Values {
		switch valueType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if target.OverflowInt(value) {
				return fmt.Errorf("native sequence value %d overflows mapped Go type %s", value, valueType)
			}
		default:
			if value < 0 || target.OverflowUint(uint64(value)) {
				return fmt.Errorf("native sequence value %d overflows mapped Go type %s", value, valueType)
			}
		}
	}
	return nil
}
