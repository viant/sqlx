package load

import (
	"github.com/viant/sqlx/metadata/product/pg"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterLoad(NewSession, pg.PqSQL9().Name)
}
