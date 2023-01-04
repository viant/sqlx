package delete

import (
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"strings"
)

const (
	columnSeparator = ","
	inFragment      = " IN ("
)

//Builder represent delete DML builder
type (
	Builder struct {
		id           string
		batchSize    int
		sql          string
		valueSize    int
		valuesOffset int
	}
)

//Build builds update statement
func (b *Builder) Build(options ...option.Option) string {
	batchSize := option.Options(options).BatchSize()
	if batchSize == b.batchSize {
		return b.sql
	}
	limit := b.valuesOffset + (batchSize * b.valueSize) + (batchSize - 1)
	result := make([]byte, limit+1)
	copy(result, b.sql[:limit])
	result[limit] = ')'
	return string(result)
}

//NewBuilder return insert builder
func NewBuilder(table string, columns []string, dialect *info.Dialect, batchSize int) (*Builder, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	getter := dialect.PlaceholderGetter()
	multiColumn := len(columns) > 1
	leftOp := strings.Join(columns, ",")
	if multiColumn {
		leftOp = "(" + leftOp + ")"
	}
	rightOp := strings.Builder{}
	itemSize := 0
	for i := 0; i < batchSize; i++ {
		if i > 0 {
			rightOp.WriteString(columnSeparator)
		}
		if multiColumn {
			rightOp.WriteString("(")
		}
		for k := 0; k < len(columns); k++ {
			if k > 0 {
				rightOp.WriteString(columnSeparator)
			}
			rightOp.WriteString(getter())
		}
		if multiColumn {
			rightOp.WriteString(")")
		}
		if i == 0 {
			itemSize = rightOp.Len()
		}
	}
	result := &Builder{
		valueSize: itemSize,
		batchSize: batchSize,
		sql:       "DELETE FROM " + table + " WHERE " + leftOp + inFragment + rightOp.String() + ")",
	}
	result.valuesOffset = strings.Index(result.sql, inFragment) + len(inFragment)
	return result, nil
}
