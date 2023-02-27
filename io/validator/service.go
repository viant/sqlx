package validator

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"time"
)

type (
	Service struct {
		checks map[reflect.Type]*Checks
		mux    sync.RWMutex
	}
)

func (s *Service) checksFor(t reflect.Type, presence *option.PresenceProvider) (*Checks, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	s.mux.RLock()
	checks, ok := s.checks[t]
	s.mux.RUnlock()
	if ok {
		return checks, nil
	}
	var err error
	if checks, err = NewChecks(t, presence); err != nil {
		return nil, err
	}
	s.mux.Lock()
	s.checks[t] = checks
	s.mux.Unlock()
	return checks, nil
}

func (s *Service) Validate(ctx context.Context, db *sql.DB, any interface{}, opts ...Option) (*Validation, error) {
	var result = &Validation{}
	options := NewOptions()
	for _, opt := range opts {
		opt(options)
	}
	valueAt, count, err := io.Values(any)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return result, nil
	}
	record := valueAt(0)
	checks, err := s.checksFor(reflect.TypeOf(record), options.PresenceProvider)
	if err != nil {
		return nil, err
	}
	var ret Validation
	path := &Path{}
	s.checkNotNull(ctx, path, valueAt, count, checks.NoNull, &ret, options)
	if err = s.checkUniques(ctx, path, db, valueAt, count, checks.Unique, &ret, options); err != nil {
		return nil, err
	}
	if err = s.checkRefs(ctx, path, db, valueAt, count, checks.RefKey, &ret, options); err != nil {
		return nil, err
	}
	ret.Failed = len(ret.Violation) > 0
	return &ret, nil
}

func (s *Service) checkNotNull(ctx context.Context, path *Path, at io.ValueAccessor, count int, checks []*Check, violations *Validation, options *Options) {
	if len(checks) == 0 || !options.Required {
		return
	}
	presence := options.PresenceProvider
	for _, check := range checks {
		for i := 0; i < count; i++ {
			itemPath := path.AppendIndex(i)
			fieldPath := itemPath.AppendField(check.Field.Name)
			record := at(i)
			recordPtr := xunsafe.AsPointer(record)
			if !presence.IsFieldSet(recordPtr, int(check.Field.Index)) {
				continue
			}
			value := check.Field.Value(recordPtr)
			if isNil(value) {
				violations.AppendNotNull(fieldPath, check.Field.Name, check.ErrorMsg)
				continue
			}
		}
	}
}

func (s *Service) checkUniques(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, checks []*Check, violations *Validation, options *Options) error {
	if len(checks) == 0 || !options.CheckUnique {
		return nil
	}
	for _, check := range checks {
		if err := s.checkUnique(ctx, path, db, at, count, check, violations, options); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) checkUnique(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, check *Check, violations *Validation, options *Options) error {
	queryCtx := s.buildUniqueMatchContext(check, count, path, at, options)
	if len(queryCtx.values) == 0 {
		return nil
	}
	//build query for all values that should be unique
	reader, err := read.New(ctx, db, queryCtx.Query(), func() interface{} {
		return reflect.New(check.CheckType).Interface()
	})
	if err != nil {
		return err
	}
	var index = map[interface{}]bool{}
	err = reader.QueryAll(ctx, func(record interface{}) error {
		recordPtr := xunsafe.AsPointer(record)
		value := check.CheckField.Value(recordPtr)
		index[mapKey(value)] = true
		return nil
	}, queryCtx.values...)
	if stmt := reader.Stmt(); stmt != nil {
		_ = stmt.Close()
	}
	if err != nil {
		return err
	}
	for k := range index {
		if ctxElem := queryCtx.index[k]; ctxElem != nil {
			violations.AppendUnique(ctxElem.path, ctxElem.field, k, check.ErrorMsg)
		}
	}
	return nil
}

func (s *Service) buildUniqueMatchContext(check *Check, count int, path *Path, at io.ValueAccessor, options *Options) *queryContext {
	queryCtx := newQueryContext(check.SQL)
	presence := options.PresenceProvider
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if !presence.IsFieldSet(recordPtr, int(check.Field.Index)) {
			continue
		}
		value := check.Field.Value(recordPtr)
		if isNil(value) && !check.Required {
			continue //unique is null and not required skipping validation
		}
		queryCtx.Append(value, check.Field.Name, fieldPath)
	}
	return queryCtx
}

func (s *Service) checkRefs(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, checks []*Check, violations *Validation, options *Options) error {
	if len(checks) == 0 || !options.CheckRef {
		return nil
	}
	for _, check := range checks {
		if err := s.checkRef(ctx, path, db, at, count, check, violations, options); err != nil {
			return err
		}
	}
	return nil

}

func (s *Service) checkRef(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, check *Check, violations *Validation, options *Options) error {
	queryCtx := s.buildCheckRefQueryContext(check, count, path, at, options, violations)
	if len(queryCtx.values) == 0 {
		return nil
	}
	//build query for all values that should be unique
	reader, err := read.New(ctx, db, queryCtx.Query(), func() interface{} {
		return reflect.New(check.CheckType).Interface()
	})
	if err != nil {
		return err
	}
	var index = map[interface{}]bool{}
	err = reader.QueryAll(ctx, func(record interface{}) error {
		recordPtr := xunsafe.AsPointer(record)
		value := check.CheckField.Value(recordPtr)
		index[mapKey(value)] = true
		return nil
	}, queryCtx.values...)
	if stmt := reader.Stmt(); stmt != nil {
		_ = stmt.Close()
	}
	if err != nil {
		return err
	}
	for k, ctxElem := range queryCtx.index { //all struct index values should have value in reference table
		if !index[k] {
			violations.AppendRef(ctxElem.path, ctxElem.field, k, check.ErrorMsg)
		}
	}
	return nil
}

func (s *Service) buildCheckRefQueryContext(check *Check, count int, path *Path, at io.ValueAccessor, options *Options, violations *Validation) *queryContext {
	queryCtx := newQueryContext(check.SQL)
	presence := options.PresenceProvider
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if !presence.IsFieldSet(recordPtr, int(check.Field.Index)) {
			continue
		}
		value := check.Field.Value(recordPtr)
		if isNil(value) && !check.Required {
			continue //ref key is null and not required skipping validation
		}
		queryCtx.Append(value, check.Field.Name, fieldPath)
	}
	return queryCtx
}

func isNil(value interface{}) bool {
	switch actual := value.(type) {
	case *int, *uint, *int64, *uint64:
		ptr := (*int)(xunsafe.AsPointer(actual))
		return ptr == nil
	case *uint8:
		return actual == nil
	case *string:
		return actual == nil
	case *time.Time:
		return actual == nil
	default:
		return value == nil
	}
}

func mapKey(value interface{}) interface{} {
	switch actual := value.(type) {
	case *string:
		if actual == nil {
			return ""
		}
		return *actual
	case *int, *uint64, *int64, *uint:
		intPtr := (*int)(xunsafe.AsPointer(actual))
		if intPtr == nil {
			return 0
		}
		return *intPtr
	case *time.Time:
		if actual == nil {
			return time.Time{}
		}
		return actual
	default:
		return value
	}
}

//New creates a new validation service
func New() *Service {
	return &Service{checks: map[reflect.Type]*Checks{}}
}
