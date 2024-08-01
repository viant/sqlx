package validator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		checks map[reflect.Type]*Checks
		mux    sync.RWMutex
	}
)

func (s *Service) checksFor(t reflect.Type, setMarker *option.SetMarker) (*Checks, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	s.mux.RLock()
	checks, ok := s.checks[t]
	if ok && setMarker != nil && checks.presence != nil {
		setMarker.Marker = checks.presence.Marker
		setMarker.IdentityIndex = checks.presence.IdentityIndex
	}
	s.mux.RUnlock()
	if ok {
		return checks, nil
	}
	var err error
	if checks, err = NewChecks(t, setMarker); err != nil {
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
	recordType := reflect.TypeOf(record)
	checks, err := s.checksFor(recordType, options.SetMarker)
	if err != nil {
		return nil, err
	}
	var ret Validation
	path := &Path{}
	if options.Location != "" {
		path = path.AppendField(options.Location)
		if reflect.TypeOf(any).Kind() == reflect.Slice {
			path.IsSlice = true
		}
	}
	if err = s.checkUniques(ctx, path, db, valueAt, count, checks.Unique, &ret, options); err != nil {
		return nil, err
	}
	if err = s.checkRefs(ctx, path, db, valueAt, count, checks.RefKey, &ret, options); err != nil {
		return nil, err
	}

	if !options.Shallow {
		if err := s.validateFields(ctx, db, recordType, path, valueAt, count, &ret, opts); err != nil {
			return nil, err
		}
	}
	ret.Failed = len(ret.Violations) > 0
	ret.sort()
	return &ret, nil
}

var timeType = reflect.TypeOf(time.Time{})

type locationMapper struct {
	srcIndex  int
	rootIndex int
	itemIndex int
	isSlice   bool
}

func (s *Service) validateFields(ctx context.Context, db *sql.DB, recordType reflect.Type, path *Path, valueAt io.ValueAccessor, count int, ret *Validation, options []Option) error {
	if recordType.Kind() == reflect.Ptr {
		recordType = recordType.Elem()
		for fieldIdx := 0; fieldIdx < recordType.NumField(); fieldIdx++ {
			field := recordType.Field(fieldIdx)
			if field.PkgPath != "" {
				continue
			}
			fieldType := field.Type
			rawFieldType := fieldType
			isSlice := false
			if rawFieldType.Kind() == reflect.Slice {
				rawFieldType = rawFieldType.Elem()
				isSlice = true
			}
			if rawFieldType.Kind() == reflect.Ptr {
				rawFieldType = rawFieldType.Elem()
			}
			if rawFieldType.Kind() != reflect.Struct || rawFieldType == timeType {
				continue
			}

			sliceType := reflect.SliceOf(fieldType)
			if isSlice {
				sliceType = fieldType
			}

			var rootMapper = map[int]*locationMapper{}

			values := s.buildFieldSlice(sliceType, count, valueAt, fieldIdx, rootMapper, isSlice)
			if values == nil {
				continue
			}
			validation, err := s.Validate(ctx, db, values, append(options, WithLocation(field.Name))...)
			if err != nil {
				return err
			}

			for j := range validation.Violations {
				item := validation.Violations[j]
				destIndex := extractIndex(item.Location)
				mapped := rootMapper[destIndex]
				remapLocation(field.Name, mapped, item, path)
				ret.Violations = append(ret.Violations, item)
			}
		}
	}
	return nil
}

func remapLocation(name string, mapper *locationMapper, item *Violation, parent *Path) {
	prefix := parent.String()
	fragment := fmt.Sprintf("%v[%v]", name, mapper.srcIndex)
	if mapper.isSlice {
		item.Location = strings.Replace(item.Location, fragment, fmt.Sprintf("%s[%v].%s[%v]", prefix, mapper.rootIndex, name, mapper.itemIndex), 1)
	} else {
		item.Location = strings.Replace(item.Location, fragment, fmt.Sprintf("%s[%v].%s", prefix, mapper.rootIndex, name), 1)
	}
}

func extractIndex(text string) int {
	begin := strings.Index(text, "[")
	end := strings.Index(text, "]")
	indexValue := 0
	if begin != -1 {
		indexLiteral := text[begin+1 : end]
		indexValue, _ = strconv.Atoi(indexLiteral)
	}
	return indexValue
}

func (s *Service) buildFieldSlice(sliceType reflect.Type, count int, valueAt io.ValueAccessor, fieldIdx int, rootMapper map[int]*locationMapper, isSlice bool) interface{} {
	sliceValue := reflect.MakeSlice(sliceType, 0, count)
	for recIdx := 0; recIdx < count; recIdx++ {
		record := valueAt(recIdx)
		recordValue := reflect.ValueOf(record)
		if recordValue.Kind() == reflect.Ptr {
			recordValue = recordValue.Elem()
		}
		fieldValue := recordValue.Field(fieldIdx)
		if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
			continue
		}
		if isSlice {
			for i := 0; i < fieldValue.Len(); i++ {
				sliceValue = reflect.Append(sliceValue, fieldValue.Index(i))
				pathMapperLen := len(rootMapper)
				rootMapper[pathMapperLen] = &locationMapper{srcIndex: pathMapperLen, rootIndex: recIdx, itemIndex: i, isSlice: true}
			}
		} else {
			sliceValue = reflect.Append(sliceValue, fieldValue)
			pathMapperLen := len(rootMapper)
			rootMapper[pathMapperLen] = &locationMapper{srcIndex: pathMapperLen, rootIndex: recIdx, itemIndex: 0}
		}
	}
	if sliceValue.Len() == 0 {
		return nil
	}
	return sliceValue.Interface()
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
	reader, err := read.New(ctx, db, queryCtx.QueryWithExclusions(), func() interface{} {
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
	setMarker := options.SetMarker
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if setMarker != nil && !setMarker.IsSet(recordPtr, int(setMarker.Marker.Index(check.Field.Name))) {
			continue
		}
		value := check.Field.Value(recordPtr)
		if isNil(value) {
			continue //unique is null and not required skipping validation
		}
		queryCtx.Append(value, check.Field.Name, fieldPath)

		if check.IdentityColumn != nil {
			queryCtx.AddExclusion([]*io.Column{check.IdentityColumn}, recordPtr, itemPath)
		}
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
	//we do not check 0 references
	for refValue, ctxElem := range queryCtx.index { //all struct index values should have value in reference table
		if !index[refValue] {
			violations.AppendRef(ctxElem.path, ctxElem.field, refValue, check.ErrorMsg)
		}
	}
	return nil
}

func (s *Service) buildCheckRefQueryContext(check *Check, count int, path *Path, at io.ValueAccessor, options *Options, violations *Validation) *queryContext {
	queryCtx := newQueryContext(check.SQL)
	setMarker := options.SetMarker
	for i := 0; i < count; i++ {
		itemPath := path.AppendIndex(i)
		fieldPath := itemPath.AppendField(check.Field.Name)
		record := at(i)
		recordPtr := xunsafe.AsPointer(record)
		if setMarker != nil && !setMarker.IsSet(recordPtr, int(setMarker.Marker.Index(check.Field.Name))) {
			continue
		}
		value := check.Field.Value(recordPtr)
		if isNil(value) {
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

// New creates a new validation service
func New() *Service {
	return &Service{checks: map[reflect.Type]*Checks{}}
}
