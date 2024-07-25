package validator

import (
	"github.com/viant/sqlx/option"
)

type (
	Options struct {
		CheckUnique bool
		CheckRef    bool
		Location    string
		Shallow     bool
		SetMarker   *option.SetMarker
	}
	Option func(c *Options)
)

func WithSetMarker() Option {
	return func(c *Options) {
		c.SetMarker = &option.SetMarker{}
	}
}

// WithUnique with unique option
func WithUnique(flag bool) Option {
	return func(c *Options) {
		c.CheckUnique = flag
	}
}

// WithRef with ref key option
func WithRef(flag bool) Option {
	return func(c *Options) {
		c.CheckRef = flag
	}
}

// WithLocation creates with location option
func WithLocation(location string) Option {
	return func(c *Options) {
		c.Location = location
	}
}

// WithShallow with shallow option
func WithShallow(flag bool) Option {
	return func(c *Options) {
		c.Shallow = flag
	}
}

func NewOptions() *Options {
	return &Options{
		CheckUnique: true,
		CheckRef:    true,
	}
}
