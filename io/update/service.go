package update

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
)

// Service represents updater
type Service struct {
	*config.Config
	initSession *session
	mux         sync.Mutex
	db          *sql.DB
}

func (s *Service) Exec(ctx context.Context, any interface{}, options ...option.Option) (int64, error) {
	match := option.Options(options).IfMatch()
	valueAt, count, err := io.Values(any)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		if match != nil {
			return 0, option.ErrNoMatch
		}
		return 0, nil
	}
	if match != nil && count != 1 {
		return 0, fmt.Errorf("update if-match requires one record")
	}
	record := valueAt(0)
	var sess *session
	if sess, err = s.ensureSession(record, options...); err != nil {
		return 0, err
	}
	if err = sess.begin(ctx, sess.db, options); err != nil {
		return 0, err
	}
	var rowsAffected int64
	dml := ""
	for i := 0; i < count; i++ {
		aRecord := valueAt(i)
		changed, e := s.tryUpdate(ctx, sess, aRecord, &dml, match)
		if e == nil && match != nil && changed != 1 {
			e = option.ErrNoMatch
		}
		if e != nil {
			err = e
			break
		}
		rowsAffected += changed
	}
	err = sess.end(err)
	return rowsAffected, err
}

func (s *Service) tryUpdate(ctx context.Context, sess *session, record interface{}, dml *string, match *option.IfMatch) (int64, error) {
	ok, err := sess.prepare(ctx, record, dml, match)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	if updatable, ok := record.(Updatable); ok {
		if err := updatable.OnUpdate(ctx); err != nil {
			return 0, err
		}
	}
	return sess.update(ctx, record, match)
}

func (s *Service) ensureSession(record interface{}, options ...option.Option) (*session, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	rType := reflect.TypeOf(record)
	if sess := s.initSession; sess != nil && sess.rType == rType {
		db := option.Options(options).Db()
		if db == nil {
			db = sess.db
		}
		return &session{
			rType:         rType,
			Config:        s.Config,
			binder:        sess.binder,
			columns:       sess.columns,
			identityIndex: sess.identityIndex,
			setMarker:     sess.setMarker,
			db:            db,
		}, nil
	}
	result := &session{
		rType:  rType,
		Config: s.Config,
		db:     s.db,
	}
	err := result.init(record, options...)
	if err == nil {
		s.initSession = result
	}
	return result, err
}

// New creates an updater
func New(ctx context.Context, db *sql.DB, tableName string, options ...option.Option) (*Service, error) {
	updater := &Service{
		Config: config.New(tableName),
		db:     db,
	}
	err := updater.ApplyOption(ctx, db, options...)
	return updater, err
}
