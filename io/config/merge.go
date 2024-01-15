package config

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
)

// MergeExecutor returns new merge executor for specified dialect
func MergeExecutor(dialect *info.Dialect, config info.MergeConfig) (io.MergeExecutor, error) {
	return registry.LookupMergeExecutor(dialect, config)
}
