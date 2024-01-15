package config

import (
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
)

// DummyMergerConfigFn dummy func for
func (c *Config) DummyMergerConfigFn() {}

// Config represents merger config
type Config struct {
	Strategy   info.PresetMergeStrategy
	MatchKeyFn func(entity interface{}) (interface{}, interface{}, error)
	NewRowFn   func() interface{}
	FetchSQL   string

	Update *Update
	Insert *Insert
	Delete *Delete
}

// Insert represents config for insert/upsert used by merge
type Insert struct {
	Transient      *Transient
	InsertSQL      string
	InsertStrategy info.PresetMergeInsStrategy
	LoadOptions    []loption.Option
}

// Update represents config for update used by merge
type Update struct {
	Transient      *Transient
	UpdateStrategy info.PresetMergeUpdStrategy
	UpdateSQL      string
	LoadOptions    []loption.Option
}

// Delete represents config for update used by merge
type Delete struct {
	Transient      *Transient
	DeleteStrategy info.PresetMergeDelStrategy
	DeleteSQL      string
}
