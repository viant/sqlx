package updater

import (
	"fmt"
	"github.com/viant/sqlx/metadata/info"
	"strings"
)

const (
	columnSeparator = ", "
)

//Builder represent insert DML builder
type Builder struct {
	id            string
	batchSize     int
	identityIndex int
	fragments     []string
	sqlPrefix     string
	sqlSuffix     string
	baseSize      int
}

//Build builds update statement
func (b *Builder) Build(options ...interface{}) string {

	size := b.baseSize
	for i := 0; i < b.identityIndex; i++ {
		if i > 0 {
			size += 2
		}
		size += len(b.fragments[i])
	}
	var result = make([]byte, size)
	pos := 0
	pos += copy(result[pos:], b.sqlPrefix)
	for i := 0; i < b.identityIndex; i++ {
		if i > 0 {
			pos += copy(result[pos:], columnSeparator)
		}
		pos += copy(result[pos:], b.fragments[i])
	}
	pos += copy(result[pos:], b.sqlSuffix)
	return string(result[:pos])
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
	for i, name := range columns {
		fragments[i] = name + " = " + getter()
	}
	criteria := strings.Join(fragments[identityIndex:], " AND ")
	result := &Builder{
		sqlPrefix:     "UPDATE " + table + " SET ",
		sqlSuffix:     " WHERE " + criteria,
		identityIndex: identityIndex,
		fragments:     fragments,
	}
	result.baseSize = len(result.sqlPrefix) + len(result.sqlSuffix)
	return result, nil
}
