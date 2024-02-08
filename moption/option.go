package moption

import (
	"database/sql"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/option"
)

type (
	Options struct {
		tx            *sql.Tx
		loadOptions   []loption.Option
		commonOptions option.Options
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

func WithCommonOptions(commonOptions option.Options) Option {
	return func(o *Options) {
		o.commonOptions = commonOptions
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

func (o *Options) GetCommonOptions() option.Options {
	return o.commonOptions
}
