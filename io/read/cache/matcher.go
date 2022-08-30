package cache

import "encoding/json"

type (
	AllowSmart bool

	//Index abstraction to represent data optimisation with caching and custom pagination
	Index struct {
		By      string
		SQL     string
		Ordered bool //SQL uses order by indexby column
		Args    []interface{}
		In      []interface{}
		Offset  int
		Limit   int
		OnSkip  func(values []interface{}) error

		marshalArgs []byte
		initialized bool
	}
)

func (m *Index) Init() {
	if m.initialized {
		return
	}

	m.initialized = true
	if m.Args == nil {
		m.Args = []interface{}{}
	}
}

func (m *Index) MarshalArgs() ([]byte, error) {
	if m.marshalArgs != nil {
		return m.marshalArgs, nil
	}

	var err error
	m.marshalArgs, err = json.Marshal(m.Args)
	return m.marshalArgs, err
}
