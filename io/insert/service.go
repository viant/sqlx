package insert

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert/generator"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
)

//Service represents generic db writer
type Service struct {
	tableName     string
	options       []option.Option
	cachedSession *session // The session is for caching only, never use it directly
	mux           sync.Mutex
	db            *sql.DB
}

//New creates an inserter service
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.StructColumnMapper
	}
	inserter := &Service{
		tableName: tableName,
		options:   options,
		db:        db,
	}
	return inserter, nil
}

//NextSequence resets next updateSequence
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
	sess, err := s.NewSession(ctx, record, batchSize)
	if err != nil {
		return nil, err
	}

	var batchRecordBuffer = make([]interface{}, batchSize*len(sess.columns))
	if options == nil {
		options = make(option.Options, 0)
	}
	options = append(options, sess.Dialect)
	return s.nextSequence(ctx, sess, record, batchRecordBuffer, recordCount, options)
}

//Exec runs insertService SQL
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, int64, error) {
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
	sess, err := s.NewSession(ctx, record, batchSize)
	if err != nil {
		return 0, 0, err
	}

	if options == nil {
		options = make(option.Options, 0)
	}
	options, err = s.ensurePresetIDStrategy(options, sess)
	if err != nil {
		return 0, 0, err
	}
	options = append(options, sess.Dialect)

	var batchRecordBuffer = make([]interface{}, batchSize*len(sess.columns))
	var identities = make([]interface{}, batchSize)
	defGenerator, err := generator.NewDefault(ctx, sess.Dialect, s.db, sess.info)
	if err != nil {
		return 0, 0, err
	}
	if err = defGenerator.Apply(ctx, any, sess.TableName, batchSize); err != nil {
		return 0, 0, err
	}

	presetInsertMode, err := s.getPresetInsertMode(sess, record, batchRecordBuffer)
	if err != nil {
		return 0, 0, err
	}
	var minSeqNextValue int64
	sess.shallPresetIdentities = sess.shallPresetIdentities && presetInsertMode

	if sess.shallPresetIdentities {
		sequence, err := s.nextSequence(ctx, sess, record, batchRecordBuffer, recordCount, options)
		if err != nil {
			return 0, 0, err
		}
		if sequence != nil {
			minSeqNextValue = sequence.MinValue(int64(recordCount))
		}
	}
	if !sess.shallPresetIdentities && sess.identityColumn != nil {
		sess.updateSequence(ctx, s.getSequenceName(sess))
	}
	if err = sess.begin(ctx, s.db, options); err != nil {
		return 0, 0, err
	}
	if err = sess.prepare(ctx, batchSize); err != nil {
		err = sess.end(err)
		return 0, 0, err
	}
	rowsAffected, lastInsertedID, err := sess.insert(ctx, batchRecordBuffer, valueAt, recordCount, minSeqNextValue, identities)
	err = sess.end(err)
	return rowsAffected, lastInsertedID, err
}

func (s *Service) ensurePresetIDStrategy(options []option.Option, sess *session) ([]option.Option, error) {
	presetIDStrategy := option.Options(options).PresetIDStrategy()
	if presetIDStrategy != dialect.PresetIDStrategyUndefined {
		return options, nil
	}

	if sess.Dialect.DefaultPresetIDStrategy == "" {
		return nil, fmt.Errorf("empty DefaultPresetIDStrategy")
	}

	if sess.Dialect.DefaultPresetIDStrategy != dialect.PresetIDStrategyUndefined {
		options = append(options, sess.Dialect.DefaultPresetIDStrategy)
	}

	return options, nil
}

func (s *Service) nextSequence(ctx context.Context, sess *session, record interface{}, batchRecordBuffer []interface{}, recordCount int, options []option.Option) (*sink.Sequence, error) {

	presetIDStrategy := option.Options(options).PresetIDStrategy()

	if presetIDStrategy == dialect.PresetIDStrategyUndefined {
		presetIDStrategy = sess.Dialect.DefaultPresetIDStrategy
	}

	if presetIDStrategy == "" {
		return nil, fmt.Errorf("empty DefaultPresetIDStrategy")
	}

	switch presetIDStrategy {
	case dialect.PresetIDStrategyUndefined:
		sess.shallPresetIdentities = false
		return nil, nil
	case dialect.PresetIDWithMax:
		options = append(options, s.maxIDSQLBuilder(sess))
	case dialect.PresetIDWithTransientTransaction:
		options = append(options, s.transientDMLBuilder(sess, record, batchRecordBuffer, int64(recordCount)))
	}
	sequenceName := s.getSequenceName(sess)
	options = append(options, option.NewArgs(sess.info.Catalog, sess.info.Schema, sequenceName), option.RecordCount(recordCount))
	meta := metadata.New()
	err := meta.Info(ctx, s.db, info.KindSequenceNextValue, &sess.sequence, options...)
	if err != nil {
		return nil, err
	}
	return &sess.sequence, nil
}

func (s *Service) sequenceName(sess *session) string {
	id := sess.identityColumn
	sequenceName := id.Tag().Sequence
	if sequenceName == "" {
		sequenceName = s.tableName
	}
	return sequenceName
}

func (s *Service) getPresetInsertMode(sess *session, record interface{}, batchRecordBuffer []interface{}) (bool, error) {
	sess.binder(record, batchRecordBuffer, 0, len(sess.columns))
	idPtr, err := io.Int64Ptr(batchRecordBuffer, *sess.identityColumnPos)
	if err != nil {
		return false, err
	}
	return *idPtr == 0, nil
}

func (s *Service) getSequenceName(sess *session) string {
	var sequence string
	if sess.identityColumn != nil {
		sequence = sess.identityColumn.Tag().Sequence
	}

	if sequence == "" {
		sequence = s.tableName
	}
	return sequence
}

func (s *Service) transientDMLBuilder(sess *session, record interface{}, batchRecordBuffer []interface{}, recordCount int64) func(*sink.Sequence) (*sqlx.SQL, error) {
	return func(sequence *sink.Sequence) (*sqlx.SQL, error) {
		resetAutoincrementQuery := sess.Builder.Build(option.BatchSize(1))
		resetAutoincrementQuery = sess.Dialect.EnsurePlaceholders(resetAutoincrementQuery)
		sess.binder(record, batchRecordBuffer, 0, len(sess.columns))

		values := make([]interface{}, len(sess.columns))
		copy(values, batchRecordBuffer[0:len(sess.columns)-1]) // don't copy ID pointer (last position in slice)

		oldValue := sequence.Value
		sequence.Value = sequence.NextValue(recordCount)

		if diff := sequence.Value - oldValue; diff < recordCount {
			return nil, fmt.Errorf("new next value for sequenceName %d is too small, expected >= %d but had ", sequence.Value, oldValue+recordCount)
		}

		passedValue := sequence.Value - 1 // decreasing is required for transient insert approach
		values[len(sess.columns)-1] = &passedValue

		resetAutoincrementSQL := &sqlx.SQL{
			Query: resetAutoincrementQuery,
			Args:  values,
		}
		return resetAutoincrementSQL, nil
	}
}

func (s *Service) maxIDSQLBuilder(sess *session) func() *sqlx.SQL {
	return func() *sqlx.SQL {
		return &sqlx.SQL{
			Query: "SELECT COALESCE(MAX(" + sess.Identity + "), 0) FROM " + s.tableName,
			Args:  nil,
		}
	}
}

// NewSession creates a new session
func (s *Service) NewSession(ctx context.Context, record interface{}, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.cachedSession; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		return &session{
			rType:                 rType,
			Config:                sess.Config,
			binder:                sess.binder,
			columns:               sess.columns,
			identityColumn:        sess.identityColumn,
			identityColumnPos:     sess.identityColumnPos,
			db:                    sess.db,
			batchSize:             sess.batchSize,
			info:                  sess.info,
			sequence:              sink.Sequence{IncrementBy: 1},
			shallPresetIdentities: sess.shallPresetIdentities,
		}, nil
	}

	aDialect, err := config.Dialect(ctx, s.db)
	if err != nil {
		return nil, err
	}

	metaSession, err := config.Session(ctx, s.db, aDialect)
	if err != nil {
		return nil, err
	}

	conf := config.New(s.tableName)
	conf.Dialect = aDialect

	result := &session{
		rType:     rType,
		batchSize: batchSize,
		Config:    conf,
		sequence:  sink.Sequence{IncrementBy: 1},
		info:      metaSession,
		db:        s.db,
	}
	if err := result.ApplyOption(ctx, s.db, s.options...); err != nil {
		return nil, err
	}
	err = result.init(record)
	if err == nil {
		s.cachedSession = result
	}
	return result, err
}
