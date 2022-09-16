package load

import (
	"github.com/viant/sqlx/metadata/product/bigquery"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterLoad(NewSession, bigquery.BigQuery().Name)
}
