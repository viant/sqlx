package read

import (
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
	"reflect"
)

type Option func(o *options)

type options struct {
	retry              RetryPolicy
	cacheOnly          bool
	queryScope         *QueryScope
	getRowMapper       NewRowMapper
	columnsObserver    func([]io.Column) error
	unmappedFn         io.Resolve
	cache              cache.Cache
	mapperCache        *MapperCache
	disableMapperCache DisableMapperCache
	db                 *sql.DB
	tx                 *sql.Tx
	inMatcher          *cache.ParmetrizedQuery
	cacheStats         *cache.Stats
	cacheRefresh       cache.Refresh
	inlineType         bool
	dialect            *info.Dialect
	options            []option.Option
}

func WithRowMapper(mapper NewRowMapper) Option {
	return func(o *options) {
		o.getRowMapper = mapper
	}
}

// WithColumnsObserver observes the native source schema before mapper creation,
// for both database reads and cache replay. It does not replace row mapping.
func WithColumnsObserver(observer func([]io.Column) error) Option {
	return func(o *options) { o.columnsObserver = observer }
}

func WithUnmappedFn(fn io.Resolve) Option {
	return func(o *options) {
		o.unmappedFn = fn
	}
}

func WithDialect(dialect *info.Dialect) Option {
	return func(o *options) {
		o.dialect = dialect
	}
}

func WithCache(aCache cache.Cache) Option {
	return func(o *options) {
		o.cache = aCache
	}
}

func WithMapperCache(aCache *MapperCache) Option {
	return func(o *options) {
		o.mapperCache = aCache
	}
}

func WithDisableMapperCache(disable DisableMapperCache) Option {
	return func(o *options) {
		o.disableMapperCache = disable
	}
}

func WithDB(db *sql.DB) Option {
	return func(o *options) {
		o.db = db
	}
}

// WithTx prepares database reads on a caller-owned transaction. The caller
// commits or rolls it back; Reader never closes the transaction.
func WithTx(tx *sql.Tx) Option {
	return func(o *options) { o.tx = tx }
}

func WithInlineType(inlineType bool) Option {
	return func(o *options) {
		o.inlineType = inlineType
	}
}

func WithInMatcher(inMatcher *cache.ParmetrizedQuery) Option {
	return func(o *options) {
		o.inMatcher = inMatcher
	}
}

func WithCacheStats(stats *cache.Stats) Option {
	return func(o *options) {
		o.cacheStats = stats
	}
}

func WithCacheRefresh(refresh cache.Refresh) Option {
	return func(o *options) {
		o.cacheRefresh = refresh
	}
}

func WithOptions(opts ...option.Option) Option {
	return func(o *options) {
		o.options = opts
		o.applyOptions(opts)
	}
}

func shallInlineType(db *sql.DB) bool {
	switch reflect.TypeOf(db.Driver()).String() {
	case "*bigquery.Driver":
		return true
	}

	return false
}

func (o *options) apply(options []Option) {
	for _, opt := range options {
		opt(o)
	}
	if o.db != nil {
		o.inlineType = shallInlineType(o.db)
		if o.dialect == nil {
			product := registry.MatchProduct(o.db)
			if product != nil {
				o.dialect = registry.LookupDialect(product)
			}
		}
	}
	if o.getRowMapper == nil {
		o.getRowMapper = newRowMapper
	}
}

// readOptions reads options TODO change with type Option func(o *options)
func (o *options) applyOptions(opts []option.Option) {
	if !option.Assign(opts, &o.getRowMapper) {
		o.getRowMapper = newRowMapper
	}
	option.Assign(opts, &o.unmappedFn)
	for _, anOption := range opts {
		switch actual := anOption.(type) {
		case cache.Cache:
			o.cache = actual
		case *MapperCache:
			o.mapperCache = actual
		case DisableMapperCache:
			o.disableMapperCache = actual
		case *cache.ParmetrizedQuery:
			o.inMatcher = actual
		case **cache.ParmetrizedQuery:
			o.inMatcher = *actual
		case *sql.DB:
			o.db = actual
		case cache.Refresh:
			o.cacheRefresh = actual
		case *cache.Stats:
			o.cacheStats = actual
		}
	}

}

func newOptions(opts []Option) *options {
	o := &options{}
	o.apply(opts)
	return o
}

// WithCacheOnly prohibits database fallback and requires cache.Lookup support.
func WithCacheOnly(only bool) Option          { return func(o *options) { o.cacheOnly = only } }
func WithQueryScope(scope *QueryScope) Option { return func(o *options) { o.queryScope = scope } }
