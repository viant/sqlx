package cache

import (
	"encoding/json"
	"github.com/aerospike/aerospike-client-go/types"
)

const (
	TypeReadMulti  = "warmup"
	TypeReadSingle = "lazy"
	TypeWrite      = "write"
	TypeNone       = "none"

	ErrorNone                      = ""
	ErrorTypeTimeout               = "aerospike timeout error"
	ErrorTypeServerUnavailable     = "aerospike server unavailable node"
	ErrorTypeServerGeneric         = "aerospike error occured"
	ErrorTypeCurrentlyNotAvailable = "aerospike currently not available"
)

type (
	Type       string
	ErrorType  string
	AllowSmart bool

	//ParmetrizedQuery abstraction to represent data optimisation with caching and custom pagination
	ParmetrizedQuery struct {
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

	Stats struct {
		Type           Type
		RecordsCounter int
		Key            string
		Dataset        string
		FoundWarmup    bool             `json:",omitempty"`
		FoundLazy      bool             `json:",omitempty"`
		ErrorType      string           `json:",omitempty"`
		ErrorCode      types.ResultCode `json:",omitempty"`
	}
)

func (s *Stats) Init() {
	s.Type = TypeNone
	s.RecordsCounter = 0
}

func (s *Stats) FoundAny() bool {
	return s.FoundLazy || s.FoundWarmup
}

func (m *ParmetrizedQuery) Init() {
	if m.initialized {
		return
	}

	m.initialized = true
	if m.Args == nil {
		m.Args = []interface{}{}
	}
}

func (m *ParmetrizedQuery) MarshalArgs() ([]byte, error) {
	if m.marshalArgs != nil {
		return m.marshalArgs, nil
	}

	var err error
	m.marshalArgs, err = json.Marshal(m.Args)
	return m.marshalArgs, err
}
