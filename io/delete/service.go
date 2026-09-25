package delete

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/option"
)

// Service represents deleter
type Service struct {
	*config.Config
	initSession *session
	mux         sync.Mutex
	db          *sql.DB
}

// Exec runs delete statements
func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, error) {
	if match := option.Options(options).IfMatch(); match != nil {
		return s.execIfMatch(ctx, any, match, options)
	}
	recordsFn, cnt, err := io.Iterator(any)
	if cnt == 0 {
		return 0, nil
	}

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

	rowsAffected, err := sess.delete(ctx, record, recordsFn, batchSize)
	err = sess.end(err)
	return rowsAffected, err

}

func (s *Service) execIfMatch(ctx context.Context, input interface{}, match *option.IfMatch, options []option.Option) (int64, error) {
	inputType := reflect.TypeOf(input)
	if inputType == nil {
		return 0, fmt.Errorf("delete if-match requires one record")
	}
	for inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}
	if inputType.Kind() == reflect.Slice || inputType.Kind() == reflect.Array {
		return 0, fmt.Errorf("delete if-match requires one record, not a list")
	}
	valueAt, count, err := io.Values(input)
	if err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, fmt.Errorf("delete if-match requires one record")
	}
	record := valueAt(0)
	if record == nil || (reflect.ValueOf(record).Kind() == reflect.Ptr && reflect.ValueOf(record).IsNil()) {
		return 0, fmt.Errorf("delete if-match requires a non-nil record")
	}
	if match.Value == nil {
		return 0, fmt.Errorf("delete if-match value is nil")
	}
	value := reflect.ValueOf(match.Value)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if value.IsNil() {
			return 0, fmt.Errorf("delete if-match value is nil")
		}
	}
	sess, err := s.ensureSession(record, 1)
	if err != nil {
		return 0, err
	}
	mapped, _, err := s.Mapper(record)
	if err != nil {
		return 0, err
	}
	column := ""
	for _, candidate := range mapped {
		if strings.EqualFold(candidate.Name(), strings.TrimSpace(match.Column)) {
			column = candidate.Name()
			break
		}
	}
	for _, key := range sess.columns {
		if strings.EqualFold(key.Name(), column) {
			column = ""
			break
		}
	}
	if column == "" {
		return 0, fmt.Errorf("delete if-match column %q is not a mapped non-key column", match.Column)
	}
	getter := s.Dialect.PlaceholderGetter()
	for range sess.columns {
		getter()
	}
	query := sess.Builder.Build(nil, option.BatchSize(1)) + " AND " + column + " = " + getter()
	values := make([]interface{}, len(sess.columns)+1)
	sess.binder(record, values, 0, len(sess.columns))
	values[len(sess.columns)] = match.Value
	if err := sess.begin(ctx, s.db, options); err != nil {
		return 0, err
	}
	var result sql.Result
	if sess.Transaction != nil {
		result, err = sess.Transaction.ExecContext(ctx, query, values...)
	} else {
		result, err = s.db.ExecContext(ctx, query, values...)
	}
	if err != nil {
		return 0, sess.end(err)
	}
	affected, err := result.RowsAffected()
	if err == nil && affected != 1 {
		err = option.ErrNoMatch
	}
	return affected, sess.end(err)
}

func (s *Service) ensureSession(record interface{}, batchSize int) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.initSession; sess != nil && sess.rType == rType && sess.batchSize == batchSize {
		return &session{
			rType:         rType,
			batchSize:     batchSize,
			Config:        s.Config,
			binder:        sess.binder,
			columns:       sess.columns,
			transactional: false,
			db:            sess.db,
		}, nil
	}
	result := &session{
		rType:     rType,
		Config:    s.Config,
		batchSize: batchSize,
	}
	err := result.init(record)
	if err == nil {
		s.initSession = result
	}
	return result, err
}

// New creates an deleter
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
