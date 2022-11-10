package sqlx

// SQL represents SQL query and its arguments
type SQL struct {
	Query string
	Args  []interface{}
}
