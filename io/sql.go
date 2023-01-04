package io

import "github.com/viant/sqlx/option"

//Builder represents SQL builder
type (
	Builder interface {
		Build(record interface{}, options ...option.Option) string
	}

	RecordlessBuilder interface {
		Build(options ...option.Option) string
	}

	BuilderAdapter struct {
		recordlessBuilder RecordlessBuilder
	}
)

func NewBuilderAdapter(builder RecordlessBuilder) Builder {
	return &BuilderAdapter{
		recordlessBuilder: builder,
	}
}

func (b *BuilderAdapter) Build(_ interface{}, options ...option.Option) string {
	return b.recordlessBuilder.Build(options...)
}
