package update

import (
	"bytes"
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
	"strings"
)

const (
	columnSeparator = ", "
)

// Builder represent update DML builder
type Builder struct {
	id                  string
	identityIndex       int
	fragments           []string
	sqlPrefix           string
	sqlSuffix           string
	estimatedBufferSize int
}

// Build builds update statement
func (b *Builder) Build(record interface{}, options ...option.Option) string {
	presenceProvider := option.Options(options).SetMarker()
	buffer := bytes.Buffer{}
	buffer.Grow(b.estimatedBufferSize)
	ptr := xunsafe.AsPointer(record)
	buffer.WriteString(b.sqlPrefix)
	hasCount := 0
	presenceAware := presenceProvider != nil && presenceProvider.Marker != nil
	for i := 0; i < b.identityIndex; i++ {
		if presenceAware && !presenceProvider.IsSet(ptr, i) {
			continue
		}
		if hasCount > 0 {
			buffer.WriteString(columnSeparator)
		}
		buffer.WriteString(b.fragments[i])
		hasCount++
	}
	if presenceAware && hasCount == 0 { //record has no changes no point to run update
		return ""
	}
	buffer.WriteString(b.sqlSuffix)
	return buffer.String()
}

// NewBuilder return insert builder
func NewBuilder(table string, columns []string, identityIndex int, dialect *info.Dialect) (*Builder, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	if identityIndex <= 0 {
		return nil, fmt.Errorf("identity index was empty")
	}
	var fragments = make([]string, len(columns))
	getter := dialect.PlaceholderGetter()
	fragmentSize := 0
	for i, name := range columns {
		fragments[i] = name + " = " + getter()
		fragmentSize += len(fragments[i])
	}
	criteria := strings.Join(fragments[identityIndex:], " AND ")
	result := &Builder{
		sqlPrefix:     "UPDATE " + table + " SET ",
		sqlSuffix:     " WHERE " + criteria,
		identityIndex: identityIndex,
		fragments:     fragments,
	}
	result.estimatedBufferSize = len(result.sqlPrefix) + len(result.sqlSuffix) + fragmentSize + (3 * len(fragments))
	return result, nil
}

var showSQL bool

func ShowSQL(b bool) {
	showSQL = b
}
