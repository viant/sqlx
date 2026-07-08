package info

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/info/placeholder"
	"strings"
)

//Dialect represents dialect
type Dialect struct {
	database.Product
	Placeholder         string // prepare statement placeholder, default '?', but oracle uses ':'
	PlaceholderResolver placeholder.Generator
	Transactional       bool
	Insert              dialect.InsertFeatures
	Upsert              dialect.UpsertFeatures
	Load                dialect.LoadFeature
	//LoadResolver        temp.SessionResolver
	CanAutoincrement  bool
	AutoincrementFunc string
	CanLastInsertID   bool
	CanReturning      bool //Postgress supports Returning Data From Modified Rows in one statement
	QuoteCharacter    byte
	// TODO: check if column has a space or exist in keywords in this case use quote if keyword is specified
	// i.e. normalized column on the dialect
	Keywords                  map[string]bool
	DefaultPresetIDStrategy   dialect.PresetIDStrategy
	SpecialKeywordEscapeQuote byte
}

//Dialects represents dialects
type Dialects []*Dialect

//PlaceholderGetter returns PlaceholderResolver if not nil, otherwise returns function that returns Placeholder
func (d *Dialect) PlaceholderGetter() func() string {
	if d.PlaceholderResolver != nil {
		return d.PlaceholderResolver.Resolver()
	}
	return (&placeholder.DefaultGenerator{}).Resolver()
}

//EnsurePlaceholders converts '?' to specific dialect placeholders if needed
func (d *Dialect) EnsurePlaceholders(SQL string) string {
	if d.Placeholder == placeholder.Default {
		return SQL
	}
	placeholders := indexPlaceholders(SQL)
	placeholderLen := len(placeholders)
	if placeholderLen == 0 || placeholders[0] == -1 {
		return SQL
	}
	var result = make([]byte, len(SQL)-placeholderLen+d.countPlaceholdersLen(0, placeholderLen))
	sqlPos := 0
	resultPos := 0
	getPlaceholder := d.PlaceholderGetter()
	for _, pos := range placeholders {
		fragment := SQL[sqlPos:pos]
		sqlPos = pos + 1
		resultPos += copy(result[resultPos:], fragment)
		aPlaceholder := getPlaceholder()
		resultPos += copy(result[resultPos:], aPlaceholder)
	}
	if sqlPos < len(SQL) {
		resultPos += copy(result[resultPos:], SQL[sqlPos:])
	}
	return string(result[:resultPos])
}

func (d *Dialect) countPlaceholdersLen(start, numOfPlaceholders int) int {
	if d.PlaceholderResolver != nil {
		return d.PlaceholderResolver.Len(start, numOfPlaceholders)
	}
	return (&placeholder.DefaultGenerator{}).Len(start, numOfPlaceholders)
}

func indexPlaceholders(SQL string) []int {
	index := strings.Index(SQL, placeholder.Default)
	var indexes = []int{index}
	for index+1 < len(SQL) {
		next := strings.Index(SQL[index+1:], placeholder.Default)
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
