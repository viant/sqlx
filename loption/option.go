package loption

import (
	"database/sql"
	"github.com/viant/sqlx/option"
)

type (
	Options struct {
		withUpsert    bool
		tx            *sql.Tx
		format        string
		hint          string
		commonOptions option.Options
	}

	Option func(o *Options)
)

func NewOptions(options ...Option) *Options {
	ret := &Options{}
	for _, opt := range options {
		opt(ret)
	}
	return ret
}

func WithTransaction(tx *sql.Tx) Option {
	return func(o *Options) {
		o.tx = tx
	}
}

func WithFormat(format string) Option {
	return func(o *Options) {
		o.format = format
	}
}

func WithHint(hint string) Option {
	return func(o *Options) {
		o.hint = hint
	}
}

func WithCommonOptions(commonOptions option.Options) Option {
	return func(o *Options) {
		o.commonOptions = commonOptions
	}
}

func WithUpsert() Option {
	return func(o *Options) {
		o.withUpsert = true
	}
}

func (o *Options) GetWithUpsert() bool {
	return o.withUpsert
}

func (o *Options) GetTransaction() *sql.Tx {
	return o.tx
}

func (o *Options) GetFormat() string {
	return o.format
}

func (o *Options) GetHint() string {
	return o.hint
}

func (o *Options) GetCommonOptions() option.Options {
	return o.commonOptions
}
