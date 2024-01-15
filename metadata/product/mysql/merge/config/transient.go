package config

import (
	"github.com/viant/sqlx/loption"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Transient represents the configuration of the temporary and auxiliary operation associated with the actual insert/update/delete
type Transient struct {
	TableName   string
	InitSQL     []string
	LoadOptions []loption.Option
}

// Table returns an expanded table name with a random part if needed
func (t *Transient) Table() string {
	if index := strings.Index(t.TableName, "${Rand}"); index != -1 {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		value := rnd.Uint32() % 100000000
		return strings.ReplaceAll(t.TableName, "${Rand}", strconv.Itoa(int(value)))
	}
	return t.TableName
}

// ExpandSQL returns an expanded sql
func (t *Transient) ExpandSQL(sql string) string {
	if index := strings.Index(sql, "${Table}"); index != -1 {
		table := t.Table()
		return strings.ReplaceAll(sql, "${Table}", table)
	}
	return sql
}

// InitSQLs returns slice of expanded sql
func (t *Transient) InitSQLs() []string {
	result := make([]string, len(t.InitSQL))
	for i, sql := range t.InitSQL {
		result[i] = t.ExpandSQL(sql)
	}
	return result
}
