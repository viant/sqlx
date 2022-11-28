package registry

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"sort"
	"strings"
	"sync"
)

var _registry = &registry{
	queries:  make(map[string][]info.Queries),
	products: make(map[string]*database.Product),
	dialects: make(map[string]info.Dialects),
	loads:    make(map[string]io.SessionResolver),
}

//Register register query info
func Register(queries ...*info.Query) error {
	return _registry.Register(queries...)
}

//RegisterLoad register session provider
func RegisterLoad(load io.SessionResolver, productName string) {
	_registry.RegisterLoad(load, productName)
}

//MatchLoadSession returns Session for Dialect
func MatchLoadSession(dialect *info.Dialect) io.Session {
	return _registry.loads[dialect.Product.Name](dialect)
}

//RegisterDialect register dialect
func RegisterDialect(dialect *info.Dialect) {
	_registry.RegisterDialect(dialect)
}

//Lookup lookups queries
func Lookup(product string, kind info.Kind) info.Queries {
	return _registry.Lookup(product, kind)
}

//Products access products registry
func Products() map[string]*database.Product {
	return _registry.products
}

//LookupDialect lookups dialect
func LookupDialect(product *database.Product) *info.Dialect {
	return _registry.LookupDialect(product)
}

type registry struct {
	mux      sync.Mutex
	queries  map[string][]info.Queries
	products map[string]*database.Product
	dialects map[string]info.Dialects
	loads    map[string]io.SessionResolver
}

func (r *registry) LookupDialect(product *database.Product) *info.Dialect {
	dialects, ok := r.dialects[product.Name]
	if !ok {
		return nil
	}
	var result *info.Dialect
	for _, candidate := range dialects {
		if product.Equal(&candidate.Product) {
			return candidate
		}
		if candidate.Major <= product.Major {
			if result == nil { // TODO IF WE DON'T HAVE THE SAME MAJOR VERSION, WE DON'T GET MAX(candidate.Major)
				result = candidate
			}
			if candidate.Major == product.Major {
				if candidate.Minor <= product.Minor {
					result = candidate
					continue
				}
				break
			}
		}
	}
	if result == nil {
		return dialects[0]
	}
	return result
}

func (r *registry) RegisterDialect(dialect *info.Dialect) {
	r.mux.Lock()
	defer r.mux.Unlock()
	dialects, ok := r.dialects[dialect.Name]
	if !ok {
		r.dialects[dialect.Name] = []*info.Dialect{dialect}
		return
	}
	for _, item := range dialects {
		if item.Product.Equal(&dialect.Product) {
			return
		}
	}
	r.dialects[dialect.Name] = append(r.dialects[dialect.Name], dialect)
	sort.Sort(r.dialects[dialect.Name])
}

func (r *registry) Lookup(product string, kind info.Kind) info.Queries {
	byKind, ok := r.queries[product]
	if !ok {
		return nil
	}
	return byKind[kind]
}

func (r *registry) Register(queries ...*info.Query) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	for i, query := range queries {
		err := query.Criteria.Validate(query.Kind)
		if err != nil {
			return err
		}

		if _, ok := r.queries[query.Product.Name]; !ok {
			r.queries[query.Product.Name] = make([]info.Queries, info.KindReserved+1)
		}
		if _, ok := r.products[query.Product.Name]; !ok {
			r.products[strings.ToLower(query.Product.Name)] = &query.Product
		}
		if query.Kind == info.KindVersion {
			r.products[strings.ToLower(query.Product.Name)] = &query.Product
		}
		r.queries[query.Product.Name][query.Kind] = append(r.queries[query.Product.Name][query.Kind], queries[i])
		sort.Sort(r.queries[query.Product.Name][query.Kind])
	}
	return nil
}

func (r *registry) RegisterLoad(load io.SessionResolver, product string) {
	r.loads[product] = load
}
