package info

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/database"
)

// Query represents dialect metadata queries
type (
	//Query represents dictionary query
	Query struct {
		Kind     Kind
		SQL      string
		Criteria Criteria
		database.Product
		PreHandlers  []Handler
		PostHandlers []Handler
	}

	// Handler interface for handling pre- and post-query custom functions
	Handler interface {
		Handle(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (doNext bool, err error)
		CanUse(options ...interface{}) bool
	}

	//Criterion represents query criterion
	Criterion struct {
		Name   string
		Column string
	}

	//Criteria represents Criterion collection
	Criteria []*Criterion

	//Queries represents querties
	Queries []*Query
)

// DefaultHandler represents default handler, implements Handler interface
type DefaultHandler struct {
	fn func(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (doNext bool, err error)
}

// Handle default implementation Handler's Handle function
func (h *DefaultHandler) Handle(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (doNext bool, err error) {
	return h.fn(ctx, db, target, options...)
}

// CanUse default implementation Handler's CanUse function
func (h *DefaultHandler) CanUse(options ...interface{}) bool {
	return true
}

// NewHandler creates new DefaultHandler
func NewHandler(fn func(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (doNext bool, err error)) *DefaultHandler {
	return &DefaultHandler{
		fn: fn,
	}
}

type nopHandler struct{}

// Handle default implementation Handler's Handle function
func (h *nopHandler) Handle(ctx context.Context, db *sql.DB, target interface{}, options ...interface{}) (doNext bool, err error) {
	return false, nil
}

// CanUse default implementation Handler's CanUse function
func (h *nopHandler) CanUse(options ...interface{}) bool {
	return true
}

var nop = &nopHandler{}

// NopHandler returns nop handler
func NopHandler() Handler {
	return nop
}

// OnPost sets Query's PostHandlers
func (q *Query) OnPost(auxiliaries ...Handler) *Query {
	q.PostHandlers = auxiliaries
	return q
}

// OnPre sets Query's PreHandlers
func (q *Query) OnPre(auxiliaries ...Handler) *Query {
	q.PreHandlers = auxiliaries
	return q
}

// NewQuery creates a new query
func NewQuery(kind Kind, SQL string, info database.Product, criteria ...*Criterion) *Query {
	return &Query{
		Kind:     kind,
		SQL:      SQL,
		Product:  info,
		Criteria: criteria,
	}
}

// Supported returns true if supported
func (c Criteria) Supported() int {
	supported := 0
	for _, item := range c {
		if item.Column != "" {
			supported++
		}
	}
	return supported
}

// Validate validates criteria kind
func (c Criteria) Validate(kind Kind) error {
	criteria := kind.Criteria()
	if len(c) != len(criteria) {
		return fmt.Errorf("invalid query '%v': expected %v criteria, but query defined %v", kind, len(criteria), len(c))
	}
	for i, item := range c {
		if item.Name != criteria[i] {
			return fmt.Errorf("invalid query criterion for kind '%v': expected %v, but had %v", kind, criteria[i], item.Name)
		}
	}
	return nil
}

func (q Queries) Len() int {
	return len(q)
}

// Swap is part of sort.Interface.
func (q Queries) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// Less is part of sort.Interface.
func (q Queries) Less(i, j int) bool {
	return q[i].Product.Major < q[j].Product.Major && q[i].Product.Minor < q[j].Product.Minor
}

// Match matches queries for version, or latest version
func (q Queries) Match(info *database.Product) *Query {
	switch len(q) {
	case 0:
		return nil
	case 1:
		return q[0]
	}
	for _, candidate := range q {
		if candidate.Product.Major >= info.Major {
			if candidate.Product.Minor >= info.Minor {
				return candidate
			}
		}
	}
	//by default return the latest version
	return q[len(q)-1]
}

// NewCriterion creates a new criteria, name refers to kind.Crtiera, column to local vendor column, use '?' for already defined placeholder, %v for substitution
func NewCriterion(name, column string) *Criterion {
	return &Criterion{
		Name:   name,
		Column: column,
	}
}
