package cache

type Entry struct {
	cache *Cache

	URL  string
	SQL  string
	Args []interface{}
	Data [][]interface{}

	found bool
	index int
}

func (e *Entry) AddRow(values []interface{}) {
	dereferenced := make([]interface{}, len(values))
	for i, value := range values {
		dereferenced[i] = e.cache.scanTypes[i].Deref(value)
	}

	e.Data = append(e.Data, dereferenced)
}

func (e *Entry) Next() bool {
	return e.index < len(e.Data)
}

func (e *Entry) Scan(values ...interface{}) (err error) {
	for i, cachedValue := range e.Data[e.index] {
		if err = e.cache.scanners[i].Scan(values[i], cachedValue); err != nil {
			return err
		}
	}

	e.index++
	return err
}

func (e *Entry) Has() bool {
	return e.found
}
