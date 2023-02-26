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
		validations map[reflect.Type]*Validation
		mux         sync.RWMutex
	}
)

func (s *Service) validationFor(t reflect.Type, presence *option.PresenceProvider) (*Validation, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	s.mux.RLock()
	validation, ok := s.validations[t]
	s.mux.RUnlock()
	if ok {
		return validation, nil
	}
	var err error
	if validation, err = NewValidation(t, presence); err != nil {
		return nil, err
	}
	s.mux.Lock()
	s.validations[t] = validation
	s.mux.Unlock()
	return validation, nil
}

func (s *Service) Validate(ctx context.Context, db *sql.DB, any interface{}, opts ...Option) error {
	options := NewOptions()
	for _, opt := range opts {
		opt(options)
	}
	valueAt, count, err := io.Values(any)
	if err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	record := valueAt(0)
	validation, err := s.validationFor(reflect.TypeOf(record), options.PresenceProvider)
	if err != nil {
		return err
	}
	var error Error
	path := &Path{}
	s.checkNotNull(ctx, path, valueAt, count, validation.NoNull, &error, options)
	if err = s.checkUniques(ctx, path, db, valueAt, count, validation.Unique, &error, options); err != nil {
		return err
	}
	if err = s.checkRefs(ctx, path, db, valueAt, count, validation.RefKey, &error, options); err != nil {
		return err
	}
	if len(error.Violation) == 0 {
		return nil
	}
	return &error
}

func (s *Service) checkNotNull(ctx context.Context, path *Path, at io.ValueAccessor, count int, checks []*Check, violations *Error, options *Options) {
	if len(checks) == 0 || !options.CheckNotNull {
		return
	}

	for _, check := range checks {
		for i := 0; i < count; i++ {
			itemPath := path.AppendIndex(i)
			fieldPath := itemPath.AppendField(check.Field.Name)
			record := at(i)
			recordPtr := xunsafe.AsPointer(record)
			if !options.IsFieldSet(recordPtr, int(check.Field.Index)) {
				continue
			}
			value := check.Field.Value(recordPtr)
			switch actual := value.(type) {
			case *int, *uint, *int64, *uint64:
				ptr := (*int)(xunsafe.AsPointer(actual))
				if ptr == nil {
					violations.AppendNotNull(fieldPath, check.Field.Name, "")
				}
			case *uint8:
				if actual == nil {
					violations.AppendNotNull(fieldPath, check.Field.Name, "")
				}
			case *string:
				if actual == nil {
					violations.AppendNotNull(fieldPath, check.Field.Name, "")
				}
			case *time.Time:
				if actual == nil {
					violations.AppendNotNull(fieldPath, check.Field.Name, "")
				}
			default:
				if value == nil {
					violations.AppendNotNull(fieldPath, check.Field.Name, "")
				}
			}
		}
	}
}

func (s *Service) checkUniques(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, checks []*Check, violations *Error, options *Options) error {
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

func (s *Service) checkUnique(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, check *Check, violations *Error, options *Options) error {
	var queryCtx = queryContext{SQL: check.SQL}
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if !options.IsFieldSet(recordPtr, int(check.Field.Index)) {
			continue
		}
		value := check.Field.Value(recordPtr)
		queryCtx.Append(value, check.Field.Name, fieldPath)
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
	_ = reader.Stmt().Close()
	if err != nil {
		return err
	}
	for k := range index {
		if ctxElem := queryCtx.index[k]; ctxElem != nil {
			violations.AppendUnique(ctxElem.path, ctxElem.field, k, "")
		}
	}
	return nil
}

func (s *Service) checkRefs(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, checks []*Check, violations *Error, options *Options) error {
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

func (s *Service) checkRef(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, check *Check, violations *Error, options *Options) error {
	var queryCtx = queryContext{SQL: check.SQL}
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if !options.IsFieldSet(recordPtr, int(check.Field.Index)) {
			continue
		}
		value := check.Field.Value(recordPtr)
		queryCtx.Append(value, check.Field.Name, fieldPath)
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
	_ = reader.Stmt().Close()
	if err != nil {
		return err
	}
	for k, ctxElem := range queryCtx.index { //all struct index values should have value in reference table
		if !index[k] {
			violations.AppendRef(ctxElem.path, ctxElem.field, k, "")
		}
	}
	return nil
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
	return &Service{validations: map[reflect.Type]*Validation{}}
}
