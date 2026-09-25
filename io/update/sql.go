package update

import (
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"github.com/viant/sqlx/io/errx"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
)

const (
	columnSeparator = ", "
)

// Builder represent update DML builder
type Builder struct {
	table         string
	columns       []string
	identityIndex int
	dialect       *info.Dialect
}

// Build builds update statement
func (b *Builder) Build(record interface{}, options ...option.Option) string {
	presenceProvider := option.Options(options).SetMarker()
	buffer := bytes.Buffer{}
	var ptr unsafe.Pointer
	if presenceProvider != nil && presenceProvider.Marker != nil {
		ptr = xunsafe.AsPointer(record)
	}
	buffer.WriteString("UPDATE ")
	buffer.WriteString(b.table)
	buffer.WriteString(" SET ")
	getter := b.dialect.PlaceholderGetter()
	hasCount := 0
	presenceAware := presenceProvider != nil && presenceProvider.Marker != nil
	for i := 0; i < b.identityIndex; i++ {
		if presenceAware && !presenceProvider.IsSet(ptr, i) {
			continue
		}
		if hasCount > 0 {
			buffer.WriteString(columnSeparator)
		}
		buffer.WriteString(b.columns[i])
		buffer.WriteString(" = ")
		buffer.WriteString(getter())
		hasCount++
	}
	if hasCount == 0 {
		return ""
	}
	buffer.WriteString(" WHERE ")
	for i := b.identityIndex; i < len(b.columns); i++ {
		if i > b.identityIndex {
			buffer.WriteString(" AND ")
		}
		buffer.WriteString(b.columns[i])
		buffer.WriteString(" = ")
		buffer.WriteString(getter())
	}
	if match := option.Options(options).IfMatch(); match != nil {
		column := ""
		for i := 0; i < b.identityIndex; i++ {
			if strings.EqualFold(b.columns[i], strings.TrimSpace(match.Column)) {
				column = b.columns[i]
				break
			}
		}
		if column == "" {
			return ""
		}
		buffer.WriteString(" AND ")
		buffer.WriteString(column)
		buffer.WriteString(" = ")
		buffer.WriteString(getter())
	}
	return buffer.String()
}

// NewBuilder return insert builder
func NewBuilder(table string, columns []string, identityIndex int, dialect *info.Dialect) (*Builder, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	if identityIndex <= 0 {
		return nil, errx.MissingIdentity("update", table, columns, identityIndex)
	}
	result := &Builder{
		table:         table,
		columns:       append([]string(nil), columns...),
		identityIndex: identityIndex,
		dialect:       dialect,
	}
	return result, nil
}

var showSQL bool

func ShowSQL(b bool) {
	showSQL = b
}
