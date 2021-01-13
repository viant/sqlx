package metadata

import "github.com/viant/sqlx/metadata/dialect"

//Dialect represents dialect
type Dialect struct {
	Product          dialect.Product
	Placeholder      string // prepare statement placeholder, default '?', but oracle uses ':'
	Transactional    bool
	Insert           dialect.InsertFeatures
	Upsert           dialect.UpsertFeatures
	Load             dialect.LoadFeature
	CanAutoincrement bool
}

