package insert

import (
	"context"
	"fmt"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// SequenceInfo returns the native numeric sequence identity and arithmetic
// metadata without reserving values or modifying the record/database. Metadata
// resolution uses the same typed insert mapping and caller transaction as
// NextSequence. An unresolved identity is an error, never an authored fallback.
func (s *Service) SequenceInfo(ctx context.Context, records interface{}, options ...option.Option) (*sink.Sequence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
	for _, updater := range sess.recordUpdaters {
		numeric, ok := updater.(*numericSequencer)
		if !ok {
			continue
		}
		opts := []option.Option{option.SequenceTable(sess.TableName), option.SequenceIdentityOnly(true), option.NewArgs(sess.info.Catalog, sess.info.Schema, numeric.getSequenceName(sess))}
		opts = append(opts, numeric.options...)
		opts = append(opts, options...)
		opts = append(opts, sess.Dialect)
		result := &sink.Sequence{}
		if err := metadata.New().Info(ctx, sess.db, info.KindSequences, result, opts...); err != nil {
			return nil, err
		}
		if result.Name == "" || result.IncrementBy <= 0 {
			return nil, fmt.Errorf("native sequence identity is unresolved for %s", sess.TableName)
		}
		return result, nil
	}
	return nil, fmt.Errorf("not found column with sequence")
}
