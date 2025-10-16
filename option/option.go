package option

import (
	"database/sql"
	"strings"
	"sync"
	"unsafe"

	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
)

// Identity represents identity option
type Identity string

// IdentityOnly  represents identity (pk) only option
type IdentityOnly bool

// StructOrderedColumns is an option that represents the same column order as the struct field order
type StructOrderedColumns bool

// Option represents generic option
type Option interface{}

// Options represents generic options
type Options []Option

// LoadFormat represents the format of data loaded
type LoadFormat string

// LoadHint represents the bigquery.JobConfigurationLoad in json format
type LoadHint string

// OnDuplicateKeySql represents SQL suffix
type OnDuplicateKeySql string

type MetaSessionCacheKey string

// MetaSessionCache wraps a sync.Map used for caching metadata sessions
type MetaSessionCache struct {
	Map *sync.Map
}

// MetaSessionCache returns the provided metadata session cache or nil
func (o Options) MetaSessionCache() *sync.Map {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case MetaSessionCache:
			return actual.Map
		}
	}
	return nil
}

// WithMetaSessionCache creates an option carrying a metadata session cache
func WithMetaSessionCache(cache *sync.Map) Option {
	return MetaSessionCache{Map: cache}
}

// Dialect returns dialect
func (o Options) Dialect() *info.Dialect {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if aDialect, ok := candidate.(*info.Dialect); ok {
			return aDialect
		}
	}
	return nil
}

// Product returns product
func (o Options) Product() *database.Product {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if aDialect, ok := candidate.(*info.Dialect); ok {
			return &aDialect.Product
		}
		if product, ok := candidate.(*database.Product); ok {
			return product
		}
	}
	return nil
}

// BatchSize returns batch size option
func (o Options) BatchSize() int {
	if len(o) == 0 {
		return 1
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case BatchSize:
			return int(actual)
		}
	}
	return 1
}

// Db returns *sql.Db or nil
func (o Options) Db() *sql.DB {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case *sql.DB:
			return actual
		}
	}
	return nil
}

// StructOrderedColumns returns identity only option value or false
func (o Options) StructOrderedColumns() bool {
	if len(o) == 0 {
		return false
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case StructOrderedColumns:
			return bool(actual)
		}
	}
	return false
}

// IdentityOnly returns identity only option value or false
func (o Options) IdentityOnly() bool {
	if len(o) == 0 {
		return false
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case IdentityOnly:
			return bool(actual)
		}
	}
	return false
}

// SetMarker returns SetMarker option value or false
func (o Options) SetMarker() *SetMarker {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case *SetMarker:
			return actual
		}
	}
	return nil
}

// Identity returns identity column
func (o Options) Identity() string {
	if len(o) == 0 {
		return ""
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case Identity:
			return string(actual)
		}
	}
	return ""
}

// Tx returns *sql.Tx or nil
func (o Options) Tx() *sql.Tx {
	if len(o) == 0 {
		return nil
	}

	for _, candidate := range o {
		if v, ok := candidate.(*sql.Tx); ok {
			return v
		}
	}
	return nil
}

// Tag represent a annotation tag name
type Tag string

// BatchSize represents a batch size options
type BatchSize int

// Columns option to control which column to operate on
type Columns []string
type ColumnRestriction map[string]bool

func (r ColumnRestriction) CanUse(column string) bool {
	if len(r) == 0 {
		return true
	}
	return r[strings.ToLower(column)]
}

func (u Columns) Restriction() ColumnRestriction {
	var result = make(map[string]bool)
	for _, column := range u {
		result[strings.ToLower(column)] = true
	}
	return result
}

// Columns returns map of updateable columns
func (o Options) Columns() Columns {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if val, ok := candidate.(Columns); ok {
			return val
		}
	}
	return nil
}

// LoadFormat returns LoadFormat
func (o Options) LoadFormat() string {
	for _, candidate := range o {
		if val, ok := candidate.(LoadFormat); ok {
			return string(val)
		}
	}
	return "JSON"
}

// LoadHint return LoadHint
func (o Options) LoadHint() string {
	for _, candidate := range o {
		if val, ok := candidate.(LoadHint); ok {
			return string(val)
		}
	}
	return ""
}

// SQL returns sqlx.SQL
func (o Options) SQL() *sqlx.SQL {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if aDialect, ok := candidate.(*sqlx.SQL); ok {
			return aDialect
		}
	}
	return nil
}

// SequenceSQLBuilder returns sqlx.SQL
func (o Options) SequenceSQLBuilder() func(*sink.Sequence) (*sqlx.SQL, error) {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if fn, ok := candidate.(func(*sink.Sequence) (*sqlx.SQL, error)); ok {
			return fn
		}
	}
	return nil
}

// MaxIDSQLBuilder returns function returning SQL which gets max identity value from table
func (o Options) MaxIDSQLBuilder() func() *sqlx.SQL {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if fn, ok := candidate.(func() *sqlx.SQL); ok {
			return fn
		}
	}
	return nil
}

// PresetIDStrategy returns PresetIDStrategy option
func (o Options) PresetIDStrategy() dialect.PresetIDStrategy {
	for _, candidate := range o {
		if value, ok := candidate.(dialect.PresetIDStrategy); ok {
			return value
		}
	}

	return dialect.PresetIDStrategyUndefined
}

// Interfaces returns current options as interfaces
func (o Options) Interfaces() []interface{} {
	return *(*[]interface{})(unsafe.Pointer(&o))
}

// AsOptions case slice of interface to Options
func AsOptions(options []interface{}) Options {
	return *(*Options)(unsafe.Pointer(&options))
}

// Args returns *Args option
func (o Options) Args() *Args {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if value, ok := candidate.(*Args); ok {
			return value
		}
	}
	return nil
}

// RecordCount represents record count option
type RecordCount int64

// RecordCount returns record count option
func (o Options) RecordCount() int64 {
	if len(o) == 0 {
		return 1
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case RecordCount:
			return int64(actual)
		}
	}
	return 0
}

// OnDuplicateKeySql returns OnDuplicateKeySql option
func (o Options) OnDuplicateKeySql() string {
	for _, candidate := range o {
		if val, ok := candidate.(OnDuplicateKeySql); ok {
			return string(val)
		}
	}
	return ""
}
