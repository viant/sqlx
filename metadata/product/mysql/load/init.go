package load

import (
	"github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterLoad(NewSession, mysql.MySQL5().Name)
}
