package sqlx

type SQL struct {
	Query string
	Args  []interface{}
}
