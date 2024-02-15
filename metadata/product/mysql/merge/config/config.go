package config

import (
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/option"
)

// DummyMergerConfigFn dummy func for
func (c *Config) DummyMergerConfigFn() {}

// Config represents merger config
type Config struct {
	Strategy   uint8
	MatchKeyFn func(entity interface{}) (interface{}, interface{}, error)
	NewRowFn   func() interface{}
	FetchSQL   string

	Update         *Update
	Insert         *Insert
	Delete         *Delete
	OperationOrder []uint8
}

// Insert represents config for insert/upsert used by merge
type Insert struct {
	Transient      *Transient
	InsertSQL      string
	InsertStrategy uint8
	LoadOptions    []loption.Option
	Options        []option.Option
}

// Update represents config for update used by merge
type Update struct {
	Transient      *Transient
	UpdateStrategy uint8
	UpdateSQL      string
}

// Delete represents config for update used by merge
type Delete struct {
	Transient      *Transient
	DeleteStrategy uint8
	DeleteSQL      string
	Options        []option.Option
}
