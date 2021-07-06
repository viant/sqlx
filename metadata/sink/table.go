package sink

//Table represent metadata table
type Table struct {
	Catalog       string `sqlx:"TABLE_CATALOG"`
	Schema        string `sqlx:"TABLE_SCHEMA"`
	Name          string `sqlx:"TABLE_NAME"`
	Comment       string `sqlx:"TABLE_COMMENT"`
	Type          string `sqlx:"TABLE_TYPE"`
	AutoIncrement string `sqlx:"AUTO_INCREMENT"`
	CreateTime    string `sqlx:"CREATE_TIME"`
	UpdateTime    string `sqlx:"UPDATE_TIME"`
	Rows          int    `sqlx:"TABLE_ROWS"`
	Version       string `sqlx:"VERSION"`
	Engine        string `sqlx:"ENGINE"`
	SQL           string `sqlx:"SQL"`
}
