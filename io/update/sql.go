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

//Builder represent update DML builder
type Builder struct {
	id            string
	identityIndex int
	fragments     []string
	sqlPrefix     string
	sqlSuffix     string
	buffer        bytes.Buffer
}

//Build builds update statement
func (b *Builder) Build(record interface{}, options ...option.Option) string {
	presenceProvider := option.Options(options).PresenceProvider()
	b.buffer.Reset()
	ptr := xunsafe.AsPointer(record)
	b.buffer.WriteString(b.sqlPrefix)

	hasCount := 0
	presenceAware := presenceProvider != nil && presenceProvider.Holder != nil
	for i := 0; i < b.identityIndex; i++ {
		if presenceAware && !presenceProvider.Has(ptr, i) {
			continue
		}
		if hasCount > 0 {
			b.buffer.WriteString(columnSeparator)
		}
		b.buffer.WriteString(b.fragments[i])
		hasCount++
	}
	if presenceAware && hasCount == 0 { //record has no changes no point to run update
		return ""
	}
	b.buffer.WriteString(b.sqlSuffix)
	return b.buffer.String()
}

//NewBuilder return insert builder
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
	estBufferSize := len(result.sqlPrefix) + len(result.sqlSuffix) + fragmentSize + (3 * len(fragments))
	result.buffer.Grow(estBufferSize)
	return result, nil
}

var showSQL bool

func ShowSQL(b bool) {
	showSQL = b
}
