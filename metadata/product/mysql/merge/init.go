package merge

import (
	"github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
)

func init() {
	registry.RegisterMergeExecutorResolver(NewMergeExecutor, mysql.MySQL5().Name)
}
