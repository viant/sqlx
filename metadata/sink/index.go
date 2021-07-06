package sink

//Index  represent index metadata
type Index struct {
	Catalog     string `sqlx:"TABLE_CATALOG"`
	Table       string `sqlx:"TABLE_NAME"`
	Type        string `sqlx:"INDEX_TYPE"`
	TableSchema string `sqlx:"TABLE_SCHEMA"`
	Schema      string `sqlx:"INDEX_SCHEMA"`
	Position    int    `sqlx:"INDEX_POSITION"`
	Name        string `sqlx:"INDEX_NAME"`
	Unique      string `sqlx:"INDEX_UNIQUE"`
	Columns     string `sqlx:"INDEX_COLUMNS"`
	Origin      string `sqlx:"INDEX_ORIGIN"`
	Partial     string `sqlx:"INDEX_PARTIAL"`
}
