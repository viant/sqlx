package info

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
	"strings"
)

//Dialect represents dialect
type Dialect struct {
	database.Product
	Placeholder         string // prepare statement placeholder, default '?', but oracle uses ':'
	PlaceholderResolver func() func() string
	Transactional       bool
	Insert              dialect.InsertFeatures
	Upsert              dialect.UpsertFeatures
	Load                dialect.LoadFeature
	CanAutoincrement    bool
	AutoincrementFunc   string
	CanLastInsertID     bool
	CanReturning        bool //Postgress supports Returning Data From Modified Rows in one statement
	QuoteCharacter      byte
	// TODO: check if column has a space or exist in keywords in this case use quote if keyword is specified
	// i.e. normalized column on the dialect
	Keywords map[string]bool
}

//Dialects represents dialects
type Dialects []*Dialect

//PlaceholderGetter returns PlaceholderResolver if not nil, otherwise returns function that returns Placeholder
func (d *Dialect) PlaceholderGetter() func() string {
	if d.PlaceholderResolver != nil {
		return d.PlaceholderResolver()
	}
	return func() string {
		return d.Placeholder
	}
}

//EnsurePlaceholders converts '?' to specific dialect placeholders if needed
func (d *Dialect) EnsurePlaceholders(SQL string) string {
	if d.Placeholder == "?" {
		return SQL
	}
	placeholders := indexPlaceholders(SQL)
	if len(placeholders) == 0 {
		return SQL
	}
	var result = make([]byte, len(SQL)+4*len(placeholders))
	sqlPos := 0
	resultPos := 0
	getPlaceholder := d.PlaceholderGetter()
	for _, pos := range placeholders {
		fragment := SQL[sqlPos:pos]
		sqlPos = pos + 1
		resultPos += copy(result[resultPos:], fragment)
		placeholder := getPlaceholder()
		resultPos += copy(result[resultPos:], placeholder)
	}
	if sqlPos < len(SQL) {
		resultPos += copy(result[resultPos:], SQL[sqlPos:])
	}
	return string(result[:resultPos])
}

func indexPlaceholders(SQL string) []int {
	index := strings.Index(SQL, "?")
	var indexes = []int{index}
	for index+1 < len(SQL) {
		next := strings.Index(SQL[index+1:], "?")
		if next == -1 {
			break
		}
		indexes = append(indexes, index+1+next)
		index += next + 1
	}

	return indexes
}

func (a Dialects) Len() int      { return len(a) }
func (a Dialects) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Dialects) Less(i, j int) bool {
	return 100000*a[i].Major+a[i].Minor < 100000*a[j].Major+a[j].Minor
}
