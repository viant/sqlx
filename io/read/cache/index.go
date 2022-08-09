package cache

type Indexed struct {
	ColumnValue interface{}
	Data        [][]interface{}
	ReadOrder   []int
}
