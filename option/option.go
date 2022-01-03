package option

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

const (
	//TagSqlx defines sqlx annotation
	TagSqlx = "sqlx"
)

//Identity represents identity option
type Identity string

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
		if tagOpt, ok := candidate.(Tag); ok {
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

//BatchSize returns batch size option
func (o Options) BatchSize() int {
	if len(o) == 0 {
		return 1
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case BatchSize:
			return int(actual)
		}
	}
	return 1
}

//Identity returns identity column
func (o Options) Identity() string {
	if len(o) == 0 {
		return ""
	}
	for _, candidate := range o {
		switch actual := candidate.(type) {
		case Identity:
			return string(actual)
		}
	}
	return ""
}

//Tag represent a annotation tag
type Tag struct {
	Tag string
}

//NewTag creates a tag
func NewTag(tag string) *Tag {
	return &Tag{Tag: tag}
}

//BatchSize represents a batch size options
type BatchSize int
