package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
	"strings"
)

type (
	//Service represents metadata service
	Service struct {
		dialect *info.Dialect
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
		s.dialect = registry.LookupDialect(product)
		return product, nil
	}
	product, err := s.matchProduct(ctx, db)
	if err == nil {
		s.recent.db = db
		s.recent.product = product
		s.dialect = registry.LookupDialect(product)
	}
	if err != nil {
		err = fmt.Errorf("failed to detect product: %w", err)
	}
	return product, err
}

//Execute execute the metadata kind corresponding SQL
func (s *Service) Execute(ctx context.Context, db *sql.DB, kind info.Kind, options ...option.Option) (sql.Result, error) {
	var err error
	product := option.Options(options).Product()
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
func (s *Service) Info(ctx context.Context, db *sql.DB, kind info.Kind, sink Sink, options ...option.Option) error {
	var err error

	product := &database.Product{}
	if !option.Assign(options, product) {
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
	product := registry.MatchProduct(db)
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
			productTmp, err := database.Parse([]byte(version))
			if err == nil && productTmp != nil {
				if productTmp.Name != "" {
					product.Name = productTmp.Name
				}
				product.Major = productTmp.Major
				product.Minor = productTmp.Minor
				product.Release = productTmp.Release
				return product, nil
			}
			break
		}
	}
	return nil, err
}

func (s *Service) executeQuery(ctx context.Context, db *sql.DB, query *info.Query, options ...option.Option) (sql.Result, error) {
	args := &option.Args{}
	option.Assign(options, &args)
	SQL, params, err := prepareSQL(query, s.dialect.PlaceholderGetter(), args)
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

func (s *Service) runQuery(ctx context.Context, db *sql.DB, query *info.Query, sink Sink, options ...option.Option) error {
	args := &option.Args{}
	option.Assign(options, &args)
	placeholderGetter := func() string {
		return "?"
	}
	if s.dialect != nil {
		placeholderGetter = s.dialect.PlaceholderGetter()
	}
	SQL, params, err := prepareSQL(query, placeholderGetter, args)
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
		return fetchStruct(ctx, rows, value)
	}
}

func prepareSQL(query *info.Query, placeholderGetter func() string, argsOpt *option.Args) (string, []interface{}, error) {
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

	var expanded = make([]bool, len(args))
	for i, item := range args {
		if text, ok := item.(string); ok {
			expr := fmt.Sprintf("$Args[%v]", i)
			if strings.Contains(SQL, expr) {
				expanded[i] = true
				SQL = strings.ReplaceAll(SQL, expr, text)
			}
		}
	}

	var criteriaValues = make([]string, 0)
	for i := range args {
		if expanded[i] {
			continue
		}
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
				criteriaValues = append(criteriaValues, column+"="+placeholderGetter())
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
