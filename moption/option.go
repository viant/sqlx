package moption

import (
	"database/sql"
	"github.com/viant/sqlx/loption"
)

type (
	Options struct {
		tx          *sql.Tx
		loadOptions []loption.Option
	}

	Option func(o *Options)
)

func NewOptions(options ...Option) *Options {
	ret := &Options{}
	for _, item := range options {
		item(ret)
	}
	return ret
}

func WithTransaction(tx *sql.Tx) Option {
	return func(o *Options) {
		o.tx = tx
	}
}

func WithLoadOptions(loadOptionSlice []loption.Option) Option {
	return func(o *Options) {
		o.loadOptions = loadOptionSlice
	}
}

func (o *Options) Apply(opts ...Option) {
	if len(opts) == 0 {
		return
	}
	for _, opt := range opts {
		opt(o)
	}
}

func (o *Options) GetTransaction() *sql.Tx {
	return o.tx
}

func (o *Options) GetLoadOptions() []loption.Option {
	return o.loadOptions
}
