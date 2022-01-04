package insert

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert/generators"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
)

//Service represents generic db writer
type Service struct {
	*config.Config
	*session
	mux sync.Mutex
	db  *sql.DB
}

//New creates an inserter service
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.StructColumnMapper
	}
	inserter := &Service{
		Config: config.New(tableName),
		db:     db,
	}
	err := inserter.ApplyOption(ctx, db, options...)
	return inserter, err
}

//Exec runs insert SQL
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, int64, error) {
	recordsFn, _, err := io.Iterator(any)
	if err != nil {
		return 0, 0, err
	}
	record := recordsFn()
	batchSize := option.Options(options).BatchSize()
	if err = generators.NewDefault(s.Dialect, s.db, nil).Apply(ctx, any, s.TableName); err != nil {
		return 0, 0, err
	}
	var sess *session
	if sess, err = s.ensureSession(record, batchSize); err != nil {
		return 0, 0, err
	}
	if err = sess.begin(ctx, s.db, options); err != nil {
		return 0, 0, err
	}
	if err = sess.prepare(ctx, batchSize); err != nil {
		return 0, 0, err
	}
	rowsAffected, lastInsertedID, err := s.insert(ctx, s.batchSize, record, recordsFn)
	err = s.end(err)
	return rowsAffected, lastInsertedID, err
}

func (s *Service) ensureSession(record interface{}, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.session; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		return &session{
			rType:               rType,
			autoIncrement:       sess.autoIncrement,
			autoIncrementColumn: sess.autoIncrementColumn,
			Config:              s.Config,
		}, nil
	}
	result := &session{
		rType:     rType,
		batchSize: batchSize,
		Config:    s.Config,
	}
	err := result.init(record)
	if err == nil {
		s.session = result
	}
	return result, err
}
