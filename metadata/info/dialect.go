package info

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
)

//Dialect represents dialect
type Dialect struct {
	database.Product
	Placeholder             string // prepare statement placeholder, default '?', but oracle uses ':'
	Transactional           bool
	Insert                  dialect.InsertFeatures
	Upsert                  dialect.UpsertFeatures
	Load                    dialect.LoadFeature
	CanAutoincrement        bool
	AutoincrementFunc       string
	CanLastInsertId         bool
	CanReturning            bool //Postgress supports Returning Data From Modified Rows in one statement
	CustomPlaceholderGetter func() func() string
	QuoteCharacter          byte
	// TODO: check if column has a space or exist in keywords in this case use quote if keyword is specified
	// i.e. normalized column on the dialect
	Keywords map[string]bool
}

type Dialects []*Dialect

//PlaceholderGetter returns CustomPlaceholderGetter if not nil, otherwise returns function that returns Placeholder
func (d *Dialect) PlaceholderGetter() func() string {
	if d.CustomPlaceholderGetter != nil {
		return d.CustomPlaceholderGetter()
	} else {
		return func() string {
			return d.Placeholder
		}
	}
}

func (a Dialects) Len() int      { return len(a) }
func (a Dialects) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Dialects) Less(i, j int) bool {
	return 100000*a[i].Major+a[i].Minor < 100000*a[j].Major+a[j].Minor
}
