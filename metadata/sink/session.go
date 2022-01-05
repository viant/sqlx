package sink

//Session represents connection session info
type Session struct {
	PID      string `sqlx:"PID"`
	Username string `sqlx:"USER_NAME"`
	Region   string `sqlx:"REGION"`
	Catalog  string `sqlx:"CATALOG_NAME"`
	Schema   string `sqlx:"SCHEMA_NAME"`
	AppName  string `sqlx:"APP_NAME"`
}
