package insert

import (
	"context"
	"fmt"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync/atomic"
)

type numericSequencer struct {
	session               *session
	column                io.Column
	options               []option.Option
	position              int
	sequence              *sink.Sequence
	sequenceValue         *int64
	detectedPreset        bool
	shallPresetIdentities bool
}

func (n *numericSequencer) updateRecord(ctx context.Context, sess *session, record interface{}, columnValue *interface{}, recordCount int, identitiesBatched []interface{}, options []option.Option) error {
	if err := n.prepareSequenceIfNeeded(ctx, sess, record, columnValue, recordCount, identitiesBatched, options); err != nil {
		return err
	}

	if n.sequence == nil || !n.shallPresetIdentities {
		if isZero(*columnValue) {
			*columnValue = nil
		}

		return nil
	}
	if n.sequenceValue == nil {
		seqValue := n.sequence.MinValue(int64(recordCount))
		n.sequenceValue = &seqValue
	}
	nextValue := atomic.LoadInt64(n.sequenceValue)
	atomic.AddInt64(n.sequenceValue, n.sequence.IncrementBy)
	return assign(columnValue, nextValue)
}

func (n *numericSequencer) prepare(_ context.Context, options []option.Option, sess *session, _ io.ValueAccessor, _ int) ([]option.Option, error) {
	n.options = options
	return nil, nil
}

func (n *numericSequencer) nextSequence(ctx context.Context, sess *session, record interface{}, batchRecordBuffer []interface{}, recordCount int, options []option.Option) (*sink.Sequence, error) {
	options = append(n.options, options...)
	presetIDStrategy := option.Options(options).PresetIDStrategy()
	if presetIDStrategy == dialect.PresetIDStrategyUndefined {
		presetIDStrategy = sess.Dialect.DefaultPresetIDStrategy
	}

	if presetIDStrategy == "" {
		return nil, fmt.Errorf("empty DefaultPresetIDStrategy")
	}

	switch presetIDStrategy {
	case dialect.PresetIDStrategyIgnore:
		return nil, nil
	case dialect.PresetIDStrategyUndefined:
		n.shallPresetIdentities = false
		n.updateSequence(ctx, n.getSequenceName(sess))
		return nil, nil
	case dialect.PresetIDWithMax:
		options = append(options, n.maxIDSQLBuilder(sess))
	case dialect.PresetIDWithTransientTransaction:
		options = append(options, dialect.PresetIDWithTransientTransaction, n.transientDMLBuilder(sess, record, batchRecordBuffer, int64(recordCount)))
	}

	sequenceName := n.getSequenceName(sess)
	options = append(options, option.NewArgs(sess.info.Catalog, sess.info.Schema, sequenceName), option.RecordCount(recordCount))
	meta := metadata.New()

	n.sequence = &sink.Sequence{
		IncrementBy: 1,
	}

	err := meta.Info(ctx, n.session.db, info.KindSequenceNextValue, n.sequence, options...)
	if err != nil {
		return nil, err
	}

	return n.sequence, nil
}

func (n *numericSequencer) transientDMLBuilder(sess *session, record interface{}, batchRecordBuffer []interface{}, recordCount int64) func(*sink.Sequence) (*sqlx.SQL, error) {
	return func(sequence *sink.Sequence) (*sqlx.SQL, error) {
		resetAutoincrementQuery := sess.Builder.Build(record, option.BatchSize(1))
		resetAutoincrementQuery = sess.Dialect.EnsurePlaceholders(resetAutoincrementQuery)
		sess.binder(record, batchRecordBuffer, 0, len(sess.columns))

		values := make([]interface{}, len(sess.columns))
		copy(values, batchRecordBuffer[0:len(sess.columns)-1]) // don't copy ID pointer (last position in slice)

		oldValue := sequence.Value
		sequence.Value = sequence.NextValue(recordCount)

		if diff := sequence.Value - oldValue; diff < recordCount {
			return nil, fmt.Errorf("new next value for sequenceName %d is too small, expected >= %d but had ", sequence.Value, oldValue+recordCount)
		}
		passedValue := sequence.Value - sequence.IncrementBy // decreasing is required for transient insert approach //TODO confirm this should be decrement by increment by
		values[len(sess.columns)-1] = &passedValue

		resetAutoincrementSQL := &sqlx.SQL{
			Query: resetAutoincrementQuery,
			Args:  values,
		}
		return resetAutoincrementSQL, nil
	}
}

func (n *numericSequencer) maxIDSQLBuilder(sess *session) func() *sqlx.SQL {
	return func() *sqlx.SQL {
		return &sqlx.SQL{
			Query: "SELECT COALESCE(MAX(" + sess.Identity + "), 0) FROM " + n.session.TableName,
			Args:  nil,
		}
	}
}

func (n *numericSequencer) getSequenceName(sess *session) string {
	var sequence string
	if tag := n.column.Tag(); tag != nil {
		sequence = tag.Sequence
	}

	if sequence == "" {
		sequence = n.session.TableName
	}

	return sequence
}

func (n *numericSequencer) columnPosition() int {
	return n.position
}

func (n *numericSequencer) getColumn() io.Column {
	return n.column
}

func (n *numericSequencer) prepareSequenceIfNeeded(ctx context.Context, sess *session, record interface{}, columnValue *interface{}, recordCount int, identitiesBatched []interface{}, options []option.Option) error {
	if n.detectedPreset {
		return nil
	}

	n.detectedPreset = true
	if recordCount == 0 {
		return nil
	}

	isColumnZeroValue := isZero(*columnValue)
	n.shallPresetIdentities = isColumnZeroValue

	if isColumnZeroValue {
		var err error
		n.sequence, err = n.nextSequence(ctx, sess, record, identitiesBatched, recordCount, options)
		if err != nil {
			return err
		}

	} else {
		n.updateSequence(ctx, n.getSequenceName(sess))
	}

	return nil
}

func (n *numericSequencer) afterFlush(ctx context.Context, values []interface{}, identities []interface{}, rowsAffected int64, lastInsertedID int64) (int64, error) {
	if rowsAffected == 0 {
		return lastInsertedID, nil
	}

	if isZero(identities[0]) {
		return lastInsertedID, nil
	}

	sequenceValue := int64(0)
	inceremntBy := int64(1)
	if n.sequence != nil {
		sequenceValue = n.sequence.Value
		inceremntBy = n.sequence.IncrementBy
	}

	switch sequenceValue {
	case 0: //no info about sequence
		for i := 0; i < int(rowsAffected); i++ {
			identityValue := identities[i]
			if !isZero(identityValue) {
				continue
			}

			if err := assign(identityValue, lastInsertedID); err != nil {
				return 0, err
			}
		}

	case lastInsertedID:
		n.updateSequence(ctx, n.sequence.Name)
		expectedNextInsertID := (1 + rowsAffected) * inceremntBy
		if expectedNextInsertID != sequenceValue { //race condition during batch insert, skip updating IDs
			return lastInsertedID, nil
		}

		for i := 0; i < int(rowsAffected); i++ {
			if err := assign(identities[i], lastInsertedID); err != nil {
				return 0, err
			}

			lastInsertedID += inceremntBy
		}
	}

	return lastInsertedID, nil
}

func isZero(value interface{}) bool {
	switch actual := value.(type) {
	case **int:
		return *actual == nil
	case *int:
		return *actual == 0
	case **int64:
		return *actual == nil
	case *int64:
		return *actual == 0
	case *uint:
		return *actual == 0
	case **uint:
		return *actual == nil
	default:
		return reflect.ValueOf(actual).Elem().IsZero()
	}
}

func (n *numericSequencer) updateSequence(ctx context.Context, sequenceName string) {
	meta := metadata.New()
	options := []option.Option{option.NewArgs(n.session.info.Catalog, n.session.info.Schema, sequenceName), n.session.Dialect}

	if n.sequence == nil {
		n.sequence = &sink.Sequence{IncrementBy: 1}
	}
	_ = meta.Info(ctx, n.session.db, info.KindSequences, n.sequence, options...)
}

func assign(dst interface{}, value int64) error {
	switch actual := dst.(type) {
	case *int:
		*actual = int(value)
		return nil

	case **int:
		asInt := int(value)
		*actual = &asInt
		return nil

	case *uint:
		*actual = uint(value)
		return nil

	case **uint:
		asInt := uint(value)
		*actual = &asInt
		return nil

	case *int64:
		*actual = value
		return nil

	case **int64:
		*actual = &value
		return nil
	}

	dstValue := reflect.ValueOf(dst)
	srcValue := reflect.ValueOf(value)

	dstValueElemType := dstValue.Type().Elem()
	if !srcValue.Type().ConvertibleTo(dstValueElemType) {
		return fmt.Errorf("can't set value %v of type %v to type %v", value, srcValue.Type().String(), dstValue.Type().String())
	}

	dstValue.Elem().Set(srcValue.Convert(dstValueElemType))
	return nil
}
