package io

import "github.com/viant/sqlx/option"

//Builder represents SQL builder
type Builder interface {
	Build(record interface{}, options ...option.Option) string
}
