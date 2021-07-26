package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/option"
	"github.com/viant/sqlx/metadata/product/ansi"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/opts"
	"strings"
)

type (
	Service struct {
		recent
	}

	recent struct {
		db      *sql.DB
		product *database.Product
	}
)

//DetectProduct detect product for supplied *sql.DB
func (s *Service) DetectProduct(ctx context.Context, db *sql.DB) (*database.Product, error) {
	if product := s.recent.match(db); product != nil {
		return product, nil
	}
	product, err := s.matchProduct(ctx, db)
	if err == nil {
		s.recent.db = db
		s.recent.product = product
	}
	if err != nil {
		err = fmt.Errorf("failed to detect product: %w", err)
	}
	return product, err
}

//Execute execute the metadata kind corresponding SQL
func (s *Service) Execute(ctx context.Context, db *sql.DB, kind info.Kind, options ...opts.Option) (sql.Result, error) {
	var err error
	product := opts.Options(options).Product()
	if product == nil {
		if product, err = s.DetectProduct(ctx, db); err != nil {
			return nil, err
		}
	}
	queries := registry.Lookup(product.Name, kind)
	if len(queries) == 0 {
		return nil, fmt.Errorf("unsupported kind: %s for: %s", kind, product.Name)
	}
	query := queries.Match(product)
	if query == nil {
		return nil, fmt.Errorf("unsupported kind: %s, for: %sv%v", kind, product.Name, product.Major)
	}
	return s.executeQuery(ctx, db, query, options...)

}

//Info execute the metadata kind corresponding Query, result are passed to sink
func (s *Service) Info(ctx context.Context, db *sql.DB, product *database.Product, kind info.Kind, sink Sink, options ...opts.Option) error {
	var err error
	if product == nil {
		if product, err = s.DetectProduct(ctx, db); err != nil {
			return err
		}
	}
	queries := registry.Lookup(product.Name, kind)
	if len(queries) == 0 {
		return fmt.Errorf("unsupported kind: %s for: %s", kind, product.Name)
	}
	query := queries.Match(product)
	if query == nil {
		return fmt.Errorf("unsupported kind: %s, for: %sv%v", kind, product.Name, product.Major)
	}
	return s.runQuery(ctx, db, query, sink, options...)
}

func (s *Service) matchProduct(ctx context.Context, db *sql.DB) (*database.Product, error) {
	driverClass := strings.ToLower(fmt.Sprintf("%T", db.Driver()))
	var product *database.Product
	for name := range registry.Products() {
		if strings.Contains(driverClass, name) {
			product = registry.Products()[name]
		}
	}
	if product == nil {
		product = &ansi.ANSI
	}
	return s.matchVersion(ctx, db, product)
}

func (s *Service) matchVersion(ctx context.Context, db *sql.DB, product *database.Product) (*database.Product, error) {
	versionQueries := registry.Lookup(product.Name, info.KindVersion)
	if len(versionQueries) == 0 {
		return product, nil
	}
	var err error
	for _, query := range versionQueries {
		var version string
		if err = s.runQuery(ctx, db, query, &version); err == nil {
			product, _ = database.Parse([]byte(version))
			if product != nil {
				return product, nil
			}
			break
		}
	}
	return nil, err
}

func (s *Service) executeQuery(ctx context.Context, db *sql.DB, query *info.Query, options ...opts.Option) (sql.Result, error) {
	args := &option.Args{}
	opts.Assign(options, &args)
	SQL, params, err := prepareSQL(query, args)
	if err != nil {
		return nil, err
	}
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ExecContext(ctx, params...)
}

func (s *Service) runQuery(ctx context.Context, db *sql.DB, query *info.Query, sink Sink, options ...opts.Option) error {
	args := &option.Args{}
	opts.Assign(options, &args)
	SQL, params, err := prepareSQL(query, args)
	if err != nil {
		return err
	}
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var rows *sql.Rows
	if len(params) > 0 {
		rows, err = stmt.Query(params...)
	} else {
		rows, err = stmt.Query()
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	switch value := sink.(type) {
	case *string:
		return fetchToString(rows, value)
	case *[]string:
		return fetchToStrings(rows, value)
	default:
		return fetchStruct(rows, value)
	}
}

func prepareSQL(query *info.Query, argsOpt *option.Args) (string, []interface{}, error) {
	args := argsOpt.Unwrap()
	var filterArgs = make([]interface{}, 0)
	if len(args) == 0 && query.Criteria.Supported() == 0 {
		return query.SQL, filterArgs, nil
	}
	criteria := query.Kind.Criteria()

	if len(args) > len(criteria) {
		return "", filterArgs, fmt.Errorf("invalid arguments, expected: %v, but had: %v", criteria, args)
	}
	SQL := query.SQL
	var criteriaValues = make([]string, 0)
	for i := range args {
		if column := query.Criteria[i].Column; column != "" {
			switch column {
			case "%":
				SQL = fmt.Sprintf(SQL, args[i])
				fallthrough
			case "?":
				continue
			default:
				if args[i] == "" {
					continue
				}
				criteriaValues = append(criteriaValues, fmt.Sprintf("%s = ?", column))
				filterArgs = append(filterArgs, args[i])
			}
		}
	}

	if len(criteriaValues) == 0 {
		if strings.Contains(SQL, "$WHERE") {
			SQL = strings.Replace(SQL, "$WHERE", "", 1)
		}
		return SQL, filterArgs, nil
	}
	clause := strings.Join(criteriaValues, " AND ")
	if strings.Contains(query.SQL, "$WHERE") {
		return strings.Replace(SQL, "$WHERE", " WHERE "+clause+" ", 1), filterArgs, nil
	} else if strings.Contains(strings.ToLower(query.SQL), "where ") {
		return SQL + " AND " + clause, filterArgs, nil
	}
	return SQL + " WHERE " + clause, filterArgs, nil
}

//match checks if the db matched previously match product
func (r *recent) match(db *sql.DB) *database.Product {
	if r.db == db {
		return r.product
	}
	return nil
}

//New creates new metadata service
func New() *Service {
	return &Service{}
}
