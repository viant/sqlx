package load

import (
	"github.com/viant/sqlx/metadata/product/sqlserver"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterLoad(NewSession, sqlserver.SQLServer().Name)
}
