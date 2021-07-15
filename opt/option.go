package opt

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

const (
	TagSqlx = "sqlx"
)

//Option represents generic option
type Option interface{}

//Options represents generic options
type Options []Option

//Tag returns annotation tag, default sqlx
func (o Options) Tag() string {
	if len(o) == 0 {
		return TagSqlx
	}
	for _, candidate := range o {
		if tagOpt, ok := candidate.(TagOption); ok {
			return tagOpt.Tag
		}
	}
	return TagSqlx
}

//Dialect returns dialect
func (o Options) Dialect() *info.Dialect {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if dialect, ok := candidate.(*info.Dialect); ok {
			return dialect
		}
	}
	return nil
}

//Product returns product
func (o Options) Product() *database.Product {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if dialect, ok := candidate.(*info.Dialect); ok {
			return &dialect.Product
		}
		if product, ok := candidate.(*database.Product); ok {
			return product
		}
	}
	return nil
}

//BatchOption returns batch option
func (o Options) Batch() *BatchOption {
	if len(o) == 0 {
		return nil
	}
	for _, candidate := range o {
		if batch, ok := candidate.(*BatchOption); ok {
			return batch
		}
	}
	return nil
}

//TagOption represent a annotation tag
type TagOption struct {
	Tag string
}

type BatchOption struct {
	Size int
}
