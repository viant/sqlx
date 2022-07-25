package load

import (
	"github.com/viant/sqlx/metadata/product/vertica"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterLoad(NewSession, vertica.Vertica().Name)
}
