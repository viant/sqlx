package sink

//Lock represents lock
type Lock struct {
	Catalog string `sqlx:"LOCK_CATALOG"`
	Schema  string `sqlx:"LOCK_SCHEMA"`
	Table   string `sqlx:"LOCK_TABLE"`
	Name    string `sqlx:"LOCK_NAME"`
	Success int    `sqlx:"SUCCESS"`
}
