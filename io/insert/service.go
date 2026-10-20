package insert

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert/generator"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Service represents generic db writer
type Service struct {
	tableName           string
	options             []option.Option
	cachedSession       *session // The session is for caching only, never use it directly
	mux                 sync.Mutex
	db                  *sql.DB
	metaSessionCacheKey string
	metaSessionCache    *sync.Map
}

// New creates an inserter service
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.StructColumnMapper
	}

	var cacheKey option.MetaSessionCacheKey
	_ = option.Assign(options, &cacheKey)

	// optional external meta session cache passed via options
	cache := option.Options(options).MetaSessionCache()

	inserter := &Service{
		tableName:           tableName,
		options:             options,
		db:                  db,
		metaSessionCacheKey: string(cacheKey),
		metaSessionCache:    cache,
	}
	return inserter, nil
}

// NextSequence resets next updateSequencer
func (s *Service) NextSequence(ctx context.Context, any interface{}, recordCount int, options ...option.Option) (*sink.Sequence, error) {
	options = append(append([]option.Option(nil), options...), s.options...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if option.Options(options).PresetIDStrategy() == dialect.PresetIDWithReservation {
		if err := s.prepareSequenceReservation(ctx, options); err != nil {
			return nil, err
		}
	}
	valueAt, count, err := io.Values(any)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, fmt.Errorf("failed to get nexr sequene for empty %T", any)
	}

	batchSize := option.Options(options).BatchSize()
	record := valueAt(0)
	db := option.Options(options).Db()
	sess, err := s.NewSession(ctx, record, db, batchSize, options...)
	if err != nil {
		return nil, err
	}

	var batchRecordBuffer = make([]interface{}, batchSize*len(sess.columns))
	if options == nil {
		options = make(option.Options, 0)
	}
	options = append(options, sess.Dialect)

	numeric, err := sess.sequenceUpdater(options)
	if err != nil {
		return nil, err
	}
	return numeric.nextSequence(ctx, sess, record, batchRecordBuffer, recordCount, options)
}

// Exec runs insertService SQL
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (rowsAffected int64, lastInsertedID int64, err error) {
	options = append(append([]option.Option(nil), options...), s.options...)
	if options == nil {
		options = make(option.Options, 0)
	}

	valueAt, recordCount, err := io.Values(any)
	if err != nil {
		return 0, 0, err
	}
	if recordCount == 0 {
		return 0, 0, nil
	}
	batchSize := option.Options(options).BatchSize()
	// BatchSize is a maximum; preparing unused rows also wastes a database round trip.
	if recordCount < batchSize {
		batchSize = recordCount
	}

	record := valueAt(0)
	if record == nil {
		return 0, 0, fmt.Errorf("invalid record/s %T %v", any, any)
	}
	db := option.Options(options).Db()
	if db == nil {
		db = s.db
	}

	sess, err := s.newSession(ctx, record, db, batchSize, false, options...)
	if err != nil {
		return 0, 0, err
	}

	for _, updater := range sess.recordUpdaters {
		updaterOpts, err := updater.prepare(ctx, options, sess, valueAt, recordCount)
		if err != nil {
			return 0, 0, err
		}

		options = append(options, updaterOpts...)
	}

	options = append(options, sess.Dialect)

	var batchRecordBuffer = make([]interface{}, batchSize*len(sess.columns))
	var identities = make([]interface{}, batchSize*len(sess.recordUpdaters))
	needsDefaults := generator.HasDefaults(sess.rType)
	needsInfo := needsDefaults
	for _, updater := range sess.recordUpdaters {
		if numeric, ok := updater.(*numericSequencer); ok && numeric.presetRecordCount > 0 {
			needsInfo = true
		}
	}
	if needsInfo {
		if err = s.loadSessionInfo(ctx, sess, options); err != nil {
			return 0, 0, err
		}
	}
	if needsDefaults {
		defGenerator, genErr := generator.NewDefault(ctx, sess.Dialect, sess.db, sess.info, s.metaSessionCacheKey, s.metaSessionCache)
		if genErr != nil {
			return 0, 0, genErr
		}
		if err = defGenerator.Apply(ctx, any, sess.TableName, batchSize); err != nil {
			return 0, 0, err
		}
	}

	if err = sess.begin(ctx, sess.db, options); err != nil {
		return 0, 0, err
	}

	// Ensure stmt/transaction are released even if sess.insert panics.
	defer func() {
		err = sess.end(err)
	}()

	if err = sess.prepare(ctx, record, batchSize); err != nil {
		return 0, 0, err
	}

	rowsAffected, lastInsertedID, err = sess.insert(ctx, batchRecordBuffer, valueAt, recordCount, identities)
	return rowsAffected, lastInsertedID, err
}

// NewSession creates a new session
func (s *Service) NewSession(ctx context.Context, record interface{}, db *sql.DB, batchSize int, options ...option.Option) (*session, error) {
	return s.newSession(ctx, record, db, batchSize, true, options...)
}

// Exec discovers session metadata only when identity allocation or tagged defaults
// need it. Explicit allocator APIs retain their ordinary eager discovery.
func (s *Service) newSession(ctx context.Context, record interface{}, db *sql.DB, batchSize int, discoverInfo bool, options ...option.Option) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if db == nil {
		db = s.db
	}
	rType := reflect.TypeOf(record)
	if sess := s.cachedSession; sess != nil && sess.rType == rType && sess.batchSize == batchSize && sess.db == db {
		if db == nil {
			db = sess.db
		}
		newSess := &session{
			rType:        rType,
			Config:       sess.Config,
			binder:       sess.binder,
			columns:      sess.columns,
			db:           db,
			batchSize:    sess.batchSize,
			insertIgnore: sess.insertIgnore,
			info:         sess.info,
			setMarker:    sess.setMarker,
		}
		// Build fresh recordUpdaters bound to newSess so per-call state
		// (sequence, sequenceValue, detectedPreset, ...) is not shared
		// across concurrent Exec calls.
		newSess.recordUpdaters = make([]recordUpdater, 0, len(sess.recordUpdaters))
		for _, ru := range sess.recordUpdaters {
			if asNumeric, ok := ru.(*numericSequencer); ok {
				newSess.recordUpdaters = append(newSess.recordUpdaters,
					newNumericSequencer(newSess, asNumeric.getColumn(), asNumeric.columnPosition()))
				continue
			}
			newSess.recordUpdaters = append(newSess.recordUpdaters, ru)
		}
		if discoverInfo && newSess.db != nil {
			if err := s.loadSessionInfo(ctx, newSess, options); err != nil {
				return nil, err
			}
		}
		return newSess, nil
	}

	// Metadata discovery retains the connection owner and supplied product.
	// Statement Args, columns and mapper options belong to the insert operation.
	effective := option.Options(append(append([]option.Option(nil), options...), s.options...))
	var metadataOptions []option.Option
	if product := effective.Product(); product != nil {
		metadataOptions = append(metadataOptions, product)
	}
	if tx := effective.Tx(); tx != nil {
		metadataOptions = append(metadataOptions, tx)
	}
	aDialect, err := config.Dialect(ctx, db, metadataOptions...)
	if err != nil {
		return nil, err
	}

	conf := config.New(s.tableName)
	conf.Dialect = aDialect

	result := &session{
		rType:        rType,
		batchSize:    batchSize,
		insertIgnore: option.Options(s.options).InsertIgnore(),
		Config:       conf,
		db:           db,
	}
	if err = result.ApplyOption(ctx, db, s.options...); err != nil {
		return nil, err
	}

	err = result.init(record)
	if err == nil && discoverInfo {
		err = s.loadSessionInfo(ctx, result, options)
	}
	if err == nil {
		// Keep metadata loaded later by Exec on its invocation session.
		// The immutable cache must not observe that concurrent write.
		cached := *result
		s.cachedSession = &cached
	}

	return result, err
}

// loadSessionInfo uses the existing metadata owner and optional caller cache.
func (s *Service) loadSessionInfo(ctx context.Context, sess *session, options []option.Option) error {
	if sess.info != nil {
		return nil
	}
	effective := option.Options(append(append([]option.Option(nil), options...), s.options...))
	var metadataOptions []option.Option
	if product := effective.Product(); product != nil {
		metadataOptions = append(metadataOptions, product)
	}
	if tx := effective.Tx(); tx != nil {
		metadataOptions = append(metadataOptions, tx)
	}
	var metaSession *sink.Session
	var err error
	if s.metaSessionCacheKey != "" {
		metaSession, err = config.SessionCached(ctx, sess.db, sess.Dialect, s.metaSessionCacheKey, s.metaSessionCache, metadataOptions...)
	} else {
		metaSession, err = config.Session(ctx, sess.db, append(metadataOptions, sess.Dialect)...)
	}
	if err == nil {
		sess.info = metaSession
	}
	return err
}
