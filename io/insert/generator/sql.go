package generator

import (
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"strings"
)

const (
	selectFragment             = "SELECT "
	sqlxOrderColumn            = "SQLX_POS"
	unionAllFragment           = " UNION "
	coalesceFragment           = "COALESCE"
	asFragment                 = " AS "
	separatorFragment          = ","
	valuePlaceholderFragment   = "(?,"
	coalesceClosureFragment    = ")"
	sqlxOrderColumnPlaceholder = ", ?+0"
)

//NewBuilder returns default values builder
func NewBuilder(columns []sink.Column, batchSize int) *Builder {
	sb := strings.Builder{}
	itemSize := 0
	for i := 0; i < batchSize; i++ {
		if i > 0 {
			sb.WriteString(unionAllFragment)
		}

		for k, column := range columns {
			if k == 0 {
				sb.WriteString(selectFragment)
			} else {
				sb.WriteString(separatorFragment)
			}

			sb.WriteString(coalesceFragment)
			sb.WriteString(valuePlaceholderFragment)
			sb.WriteString(*column.Default)
			sb.WriteString(coalesceClosureFragment)
			sb.WriteString(asFragment)
			sb.WriteString(column.Name)
		}

		sb.WriteString(sqlxOrderColumnPlaceholder)
		sb.WriteString(asFragment)
		sb.WriteString(sqlxOrderColumn)

		if i == 0 {
			itemSize = sb.Len()
		}
	}

	return &Builder{
		sql:       sb.String(),
		itemsSize: itemSize,
		batchSize: batchSize,
	}
}

//Builder represent default value builder
type Builder struct {
	sql       string
	batchSize int
	itemsSize int
}

//Build builds default values statement
func (b Builder) Build(options ...option.Option) string {
	batchSize := option.Options(options).BatchSize()
	if batchSize == b.batchSize {
		return b.sql
	}
	limit := batchSize*b.itemsSize + (batchSize-1)*len(unionAllFragment)
	return b.sql[:limit]
}
