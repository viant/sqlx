package cache

import (
	"encoding/json"
	"fmt"
	"github.com/aerospike/aerospike-client-go/types"
	"time"
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

	//Refresh forecase cache refresh
	Refresh bool
	//ParmetrizedQuery abstraction to represent data optimisation with caching and custom pagination
	ParmetrizedQuery struct {
		By           string
		SQL          string
		IdentitySQL  string
		Ordered      bool //SQL uses order by indexby column
		Args         []interface{}
		IdentityArgs []interface{}
		In           []interface{}
		Offset       int
		Limit        int
		OnSkip       func(values []interface{}) error

		marshalArgs         []byte
		marshalIdentityArgs []byte
		initialized         bool
	}

	Stats struct {
		Type           Type
		RecordsCounter int
		Key            string
		Dataset        string
		Namespace      string
		FoundWarmup    bool             `json:",omitempty"`
		FoundLazy      bool             `json:",omitempty"`
		ErrorType      string           `json:",omitempty"`
		ErrorCode      types.ResultCode `json:",omitempty"`
		ExpiryTime     *time.Time
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
	if m.IdentitySQL != "" && m.IdentityArgs == nil {
		m.IdentityArgs = []interface{}{}
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

func (m *ParmetrizedQuery) MarshalIdentityArgs() ([]byte, error) {
	if m.marshalIdentityArgs != nil {
		return m.marshalIdentityArgs, nil
	}

	if m.IdentityArgs == nil {
		m.IdentityArgs = []interface{}{}
	}

	var err error
	m.marshalIdentityArgs, err = json.Marshal(m.IdentityArgs)
	return m.marshalIdentityArgs, err
}

func (m *ParmetrizedQuery) WarmupIdentity() (string, []interface{}, []byte, error) {
	m.Init()

	if m.IdentitySQL == "" {
		if len(m.IdentityArgs) > 0 {
			return "", nil, nil, fmt.Errorf("invalid warmup identity: identity args provided without identity SQL")
		}
		marshalArgs, err := m.MarshalArgs()
		return m.SQL, m.Args, marshalArgs, err
	}

	marshalArgs, err := m.MarshalIdentityArgs()
	if err != nil {
		return "", nil, nil, err
	}
	return m.IdentitySQL, m.IdentityArgs, marshalArgs, nil
}
