package cache

import "encoding/json"

const (
	DefaultPaginationType PaginationType = ""
	SQLPaginationType     PaginationType = "SQL"
	RecordPaginationType  PaginationType = "Record"
)

type (
	AllowSmart     bool
	PaginationType string

	Matcher struct {
		SQL     string
		Ordered bool //SQL uses order by indexby column
		Args    []interface{}
		IndexBy string
		In      []interface{}
		Offset  int
		Limit   int
		OnSkip  func(values []interface{}) error

		marshalArgs []byte
		initialized bool
	}
)

func (m *Matcher) Init() {
	if m.initialized {
		return
	}

	m.initialized = true
	if m.Args == nil {
		m.Args = []interface{}{}
	}
}

func (m *Matcher) MarshalArgs() ([]byte, error) {
	if m.marshalArgs != nil {
		return m.marshalArgs, nil
	}

	var err error
	m.marshalArgs, err = json.Marshal(m.Args)
	return m.marshalArgs, err
}
