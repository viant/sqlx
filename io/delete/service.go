package delete

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
)

//Service represents deleter
type Service struct {
	*config.Config
	*session
	mux sync.Mutex
	db  *sql.DB
}

//Exec runs delete statements
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, error) {
	recordsFn, _, err := io.Iterator(any)
	if err != nil {
		return 0, err
	}
	batchSize := option.Options(options).BatchSize()

	record := recordsFn()
	var sess *session
	if sess, err = s.ensureSession(record, batchSize); err != nil {
		return 0, err
	}
	if err = sess.begin(ctx, s.db, options); err != nil {
		return 0, err
	}

	if err = sess.prepare(ctx, batchSize); err != nil {
		return 0, err
	}

	rowsAffected, err := s.delete(ctx, record, recordsFn, batchSize)
	err = s.end(err)
	return rowsAffected, err

}

func (s *Service) ensureSession(record interface{}, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.session; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		return &session{
			rType:     rType,
			batchSize: batchSize,
			Config:    s.Config,
		}, nil
	}
	result := &session{
		rType:     rType,
		Config:    s.Config,
		batchSize: batchSize,
	}
	err := result.init(record)
	if err == nil {
		s.session = result
	}
	return result, err
}

//New creates an deleter
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	var columnMapper io.ColumnMapper
	if !option.Assign(options, &columnMapper) {
		columnMapper = io.StructColumnMapper
	}
	deleter := &Service{
		Config: config.New(tableName),
		db:     db,
	}
	err := deleter.ApplyOption(ctx, db, options...)
	return deleter, err
}
