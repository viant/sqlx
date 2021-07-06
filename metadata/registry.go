package metadata

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"sort"
	"strings"
	"sync"
)

var _registry = &queryRegistry{
	queries: make(map[string][]info.Queries),
	products: make(map[string]*database.Product),
}

func Register(queries ...*info.Query) error  {
	return _registry.Register(queries...)
}


type queryRegistry struct {
	mux     sync.Mutex
	queries map[string][]info.Queries
	products map[string]*database.Product
}

func (r *queryRegistry) Lookup(product string, kind info.Kind) info.Queries {
	byKind, ok  := r.queries[product]
	if ! ok {
		return nil
	}
	return byKind[kind]
}

func (r *queryRegistry) Register(queries ...*info.Query) error {
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
		if _, ok := r.products[query.Product.Name]; ! ok {
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
