package io

import "fmt"


//Builder represents SQL builder
type Builder interface {
	Build(options ...interface{}) string
}


//Insert represent insert DML builder
type Insert struct {
	valuesSize   int
	sql          string
	batchSize    int
	valuesOffset int
}

func (i *Insert) Build(options ...interface{}) string {
	batchSize := 1
	if len(options) == 1 {
		if value, ok := options[0].(int); ok {
			batchSize = value
		}
	}
	if batchSize == i.batchSize {
		return i.sql
	}
	limit := i.valuesOffset + (batchSize * i.valuesSize) + (batchSize - 1)
	return i.sql[:limit]
}

//NewInsert return insert builder
func NewInsert(table string, batchSize int, columns, values []string) (*Insert, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	if batchSize == 0 {
		batchSize = 1
	}
	if len(values) != len(columns) {
		return nil, fmt.Errorf("values size(%v) differs from columns size(%v)", len(values), len(columns))
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

	var buffer = make([]byte, 25+columnSize+(batchSize*valuesSize)+(batchSize-1))
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
	return &Insert{
		sql:          string(buffer[:offset]),
		valuesSize:   valuesSize,
		batchSize:    batchSize,
		valuesOffset: valuesOffset,
	}, nil
}
