package info

import (
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info/dialect"
)

//Dialect represents dialect
type Dialect struct {
	database.Product
	Placeholder      string // prepare statement placeholder, default '?', but oracle uses ':'
	Transactional    bool
	Insert           dialect.InsertFeatures
	Upsert           dialect.UpsertFeatures
	Load             dialect.LoadFeature
	CanAutoincrement bool
}

type Dialects []*Dialect

func (a Dialects) Len() int      { return len(a) }
func (a Dialects) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Dialects) Less(i, j int) bool {
	return 100000*a[i].Major+a[i].Minor < 100000*a[j].Major+a[j].Minor
}
