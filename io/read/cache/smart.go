package cache

import "encoding/json"

type (
	AllowSmart bool

	SmartMatcher struct {
		RawSQL  string
		RawArgs []interface{}
		IndexBy string
		In      []interface{}

		marshalArgs []byte
		initialized bool
	}
)

func (m *SmartMatcher) Init() {
	if m.initialized {
		return
	}

	m.initialized = true
	if m.RawArgs == nil {
		m.RawArgs = []interface{}{}
	}
}

func (m *SmartMatcher) MarshalArgs() ([]byte, error) {
	if m.marshalArgs != nil {
		return m.marshalArgs, nil
	}

	var err error
	m.marshalArgs, err = json.Marshal(m.RawArgs)
	return m.marshalArgs, err
}
