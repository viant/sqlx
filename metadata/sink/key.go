package sink

//Key represents information schema constraint key
type Key struct {
	Name              string `sqlx:"CONSTRAINT_NAME"`
	Type              string `sqlx:"CONSTRAINT_TYPE"`
	Catalog           string `sqlx:"CONSTRAINT_CATALOG"`
	Schema            string `sqlx:"CONSTRAINT_SCHEMA"`
	Table             string `sqlx:"TABLE_NAME"`
	Position          int    `sqlx:"ORDINAL_POSITION"`
	Column            string `sqlx:"COLUMN_NAME"`
	ReferenceTable    string `sqlx:"REFERENCED_TABLE_NAME"`
	ReferenceColumn   string `sqlx:"REFERENCED_COLUMN_NAME"`
	ReferenceSchema   string `sqlx:"REFERENCED_TABLE_SCHEMA"`
	ConstrainPosition int    `sqlx:"POSITION_IN_UNIQUE_CONSTRAINT"`
	OnUpdate          string `sqlx:"ON_UPDATE"`
	OnDelete          string `sqlx:"ON_DELETE"`
	OnMatch           string `sqlx:"ON_MATCH"`
}
