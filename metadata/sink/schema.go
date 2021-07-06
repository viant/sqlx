package sink

//Schema represents information schema schema
type Schema struct {
	Catalog      string `sqlx:"CATALOG_NAME"`
	Name         string `sqlx:"SCHEMA_NAME"`
	CharacterSet string `sqlx:"DEFAULT_CHARACTER_SET_NAME"`
	Collation    string `sqlx:"DEFAULT_COLLATION_NAME"`
	Path         string `sqlx:"SCHEMA_FILE|SQL_PATH"`
	Sequence     int64  `sqlx:"SCHEMA_POS"`
}
