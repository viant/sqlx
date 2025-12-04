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
	sess, err := s.NewSession(ctx, record, db, batchSize)
	if err != nil {
		return nil, err
	}

	var batchRecordBuffer = make([]interface{}, batchSize*len(sess.columns))
	if options == nil {
		options = make(option.Options, 0)
	}
	options = append(options, sess.Dialect)

	for _, updater := range sess.recordUpdaters {
		asNumeric, ok := updater.(*numericSequencer)
		if ok {
			return asNumeric.nextSequence(ctx, sess, record, batchRecordBuffer, recordCount, options)
		}
	}

	return nil, fmt.Errorf("not found column with sequence")
}

// Exec runs insertService SQL
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, int64, error) {
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

	record := valueAt(0)
	if record == nil {
		return 0, 0, fmt.Errorf("invalid record/s %T %v", any, any)
	}
	db := option.Options(options).Db()
	if db == nil {
		db = s.db
	}

	sess, err := s.NewSession(ctx, record, db, batchSize)
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
	var identities = make([]interface{}, batchSize)
	defGenerator, err := generator.NewDefault(ctx, sess.Dialect, sess.db, sess.info, s.metaSessionCacheKey, s.metaSessionCache)
	if err != nil {
		return 0, 0, err
	}

	if err = defGenerator.Apply(ctx, any, sess.TableName, batchSize); err != nil {
		return 0, 0, err
	}

	if err = sess.begin(ctx, sess.db, options); err != nil {
		return 0, 0, err
	}

	if err = sess.prepare(ctx, record, batchSize); err != nil {
		err = sess.end(err)
		return 0, 0, err
	}

	rowsAffected, lastInsertedID, err := sess.insert(ctx, batchRecordBuffer, valueAt, recordCount, identities)
	err = sess.end(err)
	return rowsAffected, lastInsertedID, err
}

// NewSession creates a new session
func (s *Service) NewSession(ctx context.Context, record interface{}, db *sql.DB, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.cachedSession; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		// Reset per-call state on cached numeric sequencers before returning
		// to avoid carrying over detected preset state between sessions.
		for _, updater := range s.cachedSession.recordUpdaters {
			if asNumeric, ok := updater.(*numericSequencer); ok {
				// reset preset/sequence flags under locks
				asNumeric.muxPreset.Lock()
				asNumeric.muxSequenceValue.Lock()
				asNumeric.detectedPreset = false
				asNumeric.sequence = nil
				asNumeric.shallPresetIdentities = true
				asNumeric.sequenceValue = nil
				asNumeric.muxSequenceValue.Unlock()
				asNumeric.muxPreset.Unlock()
			}
		}

		if db == nil {
			db = sess.db
		}
		return &session{
			recordUpdaters: s.cachedSession.recordUpdaters,
			rType:          rType,
			Config:         sess.Config,
			binder:         sess.binder,
			columns:        sess.columns,
			db:             db,
			batchSize:      sess.batchSize,
			info:           sess.info,
		}, nil
	}

	aDialect, err := config.Dialect(ctx, s.db)
	if err != nil {
		return nil, err
	}
	var metaSession *sink.Session
	if s.metaSessionCacheKey != "" {
		metaSession, err = config.SessionCached(ctx, s.db, aDialect, s.metaSessionCacheKey, s.metaSessionCache)
	} else {
		metaSession, err = config.Session(ctx, s.db, aDialect)
	}

	if err != nil {
		return nil, err
	}

	conf := config.New(s.tableName)
	conf.Dialect = aDialect

	result := &session{
		rType:     rType,
		batchSize: batchSize,
		Config:    conf,
		info:      metaSession,
		db:        s.db,
	}
	if err = result.ApplyOption(ctx, s.db, s.options...); err != nil {
		return nil, err
	}

	err = result.init(record)
	if err == nil {
		s.cachedSession = result
	}

	return result, err
}
