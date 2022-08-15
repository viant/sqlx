package config

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/sqlx/option"
)

//Dialect returns a dialect
func Dialect(ctx context.Context, db *sql.DB, opts ...option.Option) (*info.Dialect, error) {
	options := option.Options(opts)
	product := options.Product()
	if product == nil {
		var err error
		meta := metadata.New()
		product, err = meta.DetectProduct(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("missing product option: %T %v", db, err)
		}
	}
	dialect := registry.LookupDialect(product)
	if dialect == nil {
		return nil, fmt.Errorf("failed to detect dialect for product: %v", product.Name)
	}
	return dialect, nil
}
