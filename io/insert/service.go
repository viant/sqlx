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
		return 0, 0, fmt.Errorf("invalid record/s %T %v\n", any, any)
	}

	var sess *session
	if sess, err = s.ensureSession(ctx, record, batchSize); err != nil {
		return 0, 0, err
	}

	var batchRecValuesBuf = make([]interface{}, batchSize*len(sess.columns))
	var identities = make([]interface{}, batchSize)
	metaSession, err := config.Session(ctx, s.db)
	if err != nil {
		return 0, 0, err
	}

	defGenerator, err := generator.NewDefault(ctx, sess.Dialect, s.db, metaSession)
	if err != nil {
		return 0, 0, err
	}
	if err = defGenerator.Apply(ctx, any, sess.TableName, batchSize); err != nil {
		return 0, 0, err
	}

	presetInsertMode, err := s.getPresetInsertMode(sess, record, batchRecValuesBuf)
	if err != nil {
		return 0, 0, err
	}

	var sequence = &sink.Sequence{}
	var minSeqNextValue int64

	var presetIdentities = presetInsertMode && sess.shallPresetIdentities
	if presetIdentities {
		switch option.Options(options).AutoincrementStrategy() {
		case option.PresetIdStrategyUndefined:
			presetIdentities = false
		case option.PresetIdWithMax:
			options = append(options, s.maxIDSQLBuilder(sess))
		case option.PresetIdWithTransientTransaction:
			options = append(options, s.transientDMLBuilder(sess, record, batchRecValuesBuf, int64(recordCount)))
		}
		if presetIdentities {
			sequenceName := s.getSequenceName(sess)
			options = append(options, option.NewArgs(metaSession.Catalog, metaSession.Schema, sequenceName), option.RecordCount(recordCount))
			meta := metadata.New()
			err := meta.Info(ctx, s.db, info.KindSequenceNextValue, sequence, options...)
			if err != nil {
				return 0, 0, err
			}
			sess.incrementBy = int(sequence.IncrementBy)
			minSeqNextValue = sequence.MinValue(int64(recordCount))
		}
	}

	if err = sess.begin(ctx, s.db, options); err != nil {
		return 0, 0, err
	}
	if err = sess.prepare(ctx, batchSize); err != nil {
		err = sess.end(err)
		return 0, 0, err
	}

	rowsAffected, lastInsertedID, err := sess.insert(ctx, batchRecValuesBuf, valueAt, recordCount, minSeqNextValue, sequence, presetIdentities, identities)
	err = sess.end(err)
	return rowsAffected, lastInsertedID, err
}

func (s *Service) sequenceName(sess *session) string {
	id := sess.identityColumn
	sequenceName := id.Tag().Sequence
	if sequenceName == "" {
		sequenceName = s.tableName
	}
	return sequenceName
}

func (s *Service) getPresetInsertMode(sess *session, record interface{}, batchRecValuesBuf []interface{}) (bool, error) {
	sess.binder(record, batchRecValuesBuf, 0, len(sess.columns))
	idPtr, err := io.Int64Ptr(batchRecValuesBuf, *sess.identityColumnPos)
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

func (s *Service) transientDMLBuilder(sess *session, record interface{}, batchRecValuesBuf []interface{}, recordCount int64) func(*sink.Sequence) (*sqlx.SQL, error) {
	return func(sequence *sink.Sequence) (*sqlx.SQL, error) {
		resetAutoincrementQuery := sess.Builder.Build(option.BatchSize(1))
		resetAutoincrementQuery = sess.Dialect.EnsurePlaceholders(resetAutoincrementQuery)
		sess.binder(record, batchRecValuesBuf, 0, len(sess.columns))

		values := make([]interface{}, len(sess.columns))
		copy(values, batchRecValuesBuf[0:len(sess.columns)-2]) // don't copy ID pointer (last position in slice)

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
func (s *Service) ensureSession(ctx context.Context, record interface{}, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.cachedSession; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		return &session{
			rType:             rType,
			Config:            sess.Config,
			binder:            sess.binder,
			columns:           sess.columns,
			identityColumn:    sess.identityColumn,
			identityColumnPos: sess.identityColumnPos,
			db:                sess.db,
			batchSize:         sess.batchSize,
			incrementBy:       1,
		}, nil
	}
	result := &session{
		rType:       rType,
		batchSize:   batchSize,
		Config:      config.New(s.tableName),
		incrementBy: 1,
	}
	if err := result.ApplyOption(ctx, s.db, s.options...); err != nil {
		return nil, err
	}

	err := result.init(record)
	if err == nil {
		s.cachedSession = result
	}
	return result, err
}
