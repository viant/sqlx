package insert

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"strings"
)

const (
	insertIntoFragment = "INSERT INTO "
)

// Builder represent insert DML builder
type Builder struct {
	dialect    *info.Dialect
	id         string
	valuesSize int
	sql        string
	batchSize  int
	offsets    []uint32
}

// Build builds insert statement
func (b *Builder) Build(record interface{}, options ...option.Option) string {
	batchSize := option.Options(options).BatchSize()
	onDuplicateKeySql := option.Options(options).OnDuplicateKeySql()
	suffix := ""

	if onDuplicateKeySql != "" {
		suffix = " " + onDuplicateKeySql
	}

	if b.dialect.CanReturning && len(b.id) > 0 {
		suffix += " RETURNING " + b.id
	}

	if batchSize == b.batchSize {
		return b.sql + suffix
	}

	limit := b.offsets[batchSize-1]
	return b.sql[:limit] + suffix
}

// NewBuilder return insert builder
func NewBuilder(table string, columns []string, dialect *info.Dialect, identity string, batchSize int) (io.Builder, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	sqlBuilder := strings.Builder{}
	sqlBuilder.Grow(estimateBufferSize(table, columns, batchSize))
	var offsets []uint32

	sqlBuilder.WriteString(insertIntoFragment)

	escapeRune := dialect.SpecialKeywordEscapeQuote
	if escapeRune == 0 {
		escapeRune = '"'
	}

	sqlBuilder.WriteByte(escapeRune)
	sqlBuilder.WriteString(table)
	sqlBuilder.WriteByte(escapeRune)
	sqlBuilder.WriteString("(")
	for i, column := range columns {
		if i > 0 {
			sqlBuilder.WriteString(",")
		}
		sqlBuilder.WriteString(column)
	}
	sqlBuilder.WriteString(") VALUES ")
	getPlaceholder := dialect.PlaceholderGetter()
	for i := 0; i < batchSize; i++ {
		if i > 0 {
			sqlBuilder.WriteString(",")
		}
		sqlBuilder.WriteString("(")
		for j := range columns {
			if j > 0 {
				sqlBuilder.WriteString(",")
			}
			sqlBuilder.WriteString(getPlaceholder())
		}
		sqlBuilder.WriteString(")")
		offsets = append(offsets, uint32(sqlBuilder.Len()))
	}

	return &Builder{
		sql:       sqlBuilder.String(),
		dialect:   dialect,
		batchSize: batchSize,
		offsets:   offsets,
		id:        identity,
	}, nil
}

func estimateBufferSize(table string, columns []string, batchSize int) int {
	estimateSize := 0
	for _, column := range columns {
		estimateSize += len(column) + 4
	}
	return len(table) + len(insertIntoFragment) + 10 + estimateSize*batchSize
}

var showSQL bool

func ShowSQL(b bool) {
	showSQL = b
}
