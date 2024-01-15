package config

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
)

// LoadSession Returns new session for specified Dialect
func LoadSession(dialect *info.Dialect) io.LoadExecutor {
	return registry.MatchLoadSession(dialect)
}
