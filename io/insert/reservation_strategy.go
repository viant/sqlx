package insert

import (
	"context"
	"fmt"

	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// reserveFromNextSequence adapts an existing native range strategy without
// changing its SQL, transaction, session or locking implementation. In
// particular MySQL transient allocation still opens and rolls back its own tx.
func (s *Service) reserveFromNextSequence(ctx context.Context, records any, count int, options []option.Option) (*sink.Reservation, error) {
	identity, err := s.SequenceInfo(ctx, records, options...)
	if err != nil {
		return nil, err
	}
	sequence, err := s.NextSequence(ctx, records, count, options...)
	if err != nil {
		return nil, err
	}
	if sequence == nil {
		return nil, fmt.Errorf("native sequence strategy returned no allocated range")
	}
	result, err := sequence.Reservation(count)
	if err != nil {
		return nil, err
	}
	// Old handlers report their logical lock/range name. The typed native mapper
	// supplies the physical authority used for supplied-ID exclusion. No option
	// passed to the old handler is rewritten to alter its lock key or SQL.
	result.Sequence.Catalog = identity.Catalog
	result.Sequence.Schema = identity.Schema
	result.Sequence.Name = identity.Name
	return result, nil
}

func (s *session) reservationStrategy(options []option.Option) dialect.PresetIDStrategy {
	return s.Dialect.SequenceStrategy(option.Options(options).PresetIDStrategy())
}
