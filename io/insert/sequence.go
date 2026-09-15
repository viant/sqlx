package insert

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/metadata/info/dialect"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// SequenceInfo returns the native numeric sequence identity and arithmetic
// metadata without reserving values or modifying the record/counters. The
// reservation strategy acquires transaction write intent and may initialize
// native allocator metadata before reading identity. Otherwise this is read-only. Metadata
// resolution uses the same typed insert mapping and caller transaction as
// NextSequence. An unresolved identity is an error, never an authored fallback.
func (s *Service) SequenceInfo(ctx context.Context, records interface{}, options ...option.Option) (*sink.Sequence, error) {
	options = append(append([]option.Option(nil), options...), s.options...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if option.Options(options).PresetIDStrategy() == dialect.PresetIDWithReservation {
		if err := s.prepareSequenceReservation(ctx, options); err != nil {
			return nil, err
		}
	}
	at, count, err := io.Values(records)
	if err != nil {
		return nil, err
	}
	if count == 0 || at(0) == nil {
		return nil, fmt.Errorf("sequence metadata requires a typed record")
	}
	sess, err := s.NewSession(ctx, at(0), option.Options(options).Db(), option.Options(options).BatchSize(), options...)
	if err != nil {
		return nil, err
	}
	numeric, err := sess.sequenceUpdater(options)
	if err != nil {
		return nil, err
	}
	{

		opts := []option.Option{option.SequenceTable(sess.TableName), option.SequenceColumn(numeric.column.Name()), option.SequenceIdentityOnly(true), option.NewArgs(sess.info.Catalog, sess.info.Schema, numeric.getSequenceName(sess))}
		opts = append(opts, numeric.options...)
		opts = append(opts, options...)
		opts = append(opts, sess.Dialect)
		result := &sink.Sequence{}
		if err := metadata.New().Info(ctx, sess.db, info.KindSequences, result, opts...); err != nil {
			return nil, err
		}
		if result.Name == "" || result.IncrementBy == 0 {
			return nil, fmt.Errorf("native sequence identity is unresolved for %s", sess.TableName)
		}
		return result, nil
	}
}

// prepareSequenceReservation delegates capability and write-intent ownership to
// the registered product before session/identity queries can establish a snapshot.
func (s *Service) prepareSequenceReservation(ctx context.Context, options []option.Option) error {
	opts := []option.Option{option.SequenceTable(s.tableName)}
	if tx := option.Options(options).Tx(); tx != nil {
		opts = append(opts, tx)
	}
	db := option.Options(options).Db()
	if db == nil {
		db = s.db
	}
	return metadata.New().Info(ctx, db, info.KindSequenceLock, nil, opts...)
}
