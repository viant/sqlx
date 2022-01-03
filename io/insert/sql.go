package insert

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

//Builder represent insert DML builder
type Builder struct {
	dialect      *info.Dialect
	id           string
	valuesSize   int
	sql          string
	batchSize    int
	valuesOffset int
}

//Build builds insert statement
func (b *Builder) Build(options ...option.Option) string {
	batchSize := option.Options(options).BatchSize()
	suffix := ""
	if b.dialect.CanReturning && len(b.id) > 0 {
		suffix = " RETURNING " + b.id
	}
	if batchSize == b.batchSize {
		return b.sql + suffix
	}
	limit := b.valuesOffset + (batchSize * b.valuesSize) + (batchSize - 1)
	return b.sql[:limit] + suffix
}

//NewBuilder return insert builder
func NewBuilder(table string, columns []string, dialect *info.Dialect, opts ...option.Option) (io.Builder, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	options := option.Options(opts)
	batchSize := options.BatchSize()
	var values = make([]string, len(columns))
	placeholderGetter := dialect.PlaceholderGetter()
	for i := range values {
		values[i] = placeholderGetter()
	}
	columnSize := len(columns) - 1
	for _, column := range columns {
		columnSize += len(column)
	}
	valuesSize := len(values) + 1
	for _, value := range values {
		valuesSize += len(value)
	}
	var valBuffer = make([]byte, valuesSize)

	var buffer = make([]byte, 23+columnSize+(batchSize*valuesSize)+(batchSize-1)+len(table))
	offset := copy(buffer, "INSERT INTO ")
	offset += copy(buffer[offset:], table)
	offset += copy(buffer[offset:], "(")
	offset += copy(buffer[offset:], columns[0])

	valOffset := copy(valBuffer, "(")
	valOffset += copy(valBuffer[valOffset:], values[0])

	for i := 1; i < len(columns); i++ {
		offset += copy(buffer[offset:], ",")
		offset += copy(buffer[offset:], columns[i])
		valOffset += copy(valBuffer[valOffset:], ",")
		valOffset += copy(valBuffer[valOffset:], values[i])
	}
	valOffset += copy(valBuffer[valOffset:], ")")
	offset += copy(buffer[offset:], ") VALUES ")
	valuesOffset := offset
	offset += copy(buffer[offset:], valBuffer)
	for i := 1; i < batchSize; i++ {
		offset += copy(buffer[offset:], ",")
		offset += copy(buffer[offset:], valBuffer)
	}
	return &Builder{
		sql:          string(buffer[:offset]),
		valuesSize:   valuesSize,
		batchSize:    batchSize,
		valuesOffset: valuesOffset,
		dialect:      dialect,
		id:           options.Identity(),
	}, nil
}
