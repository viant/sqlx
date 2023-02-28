package validator

import (
	"github.com/viant/sqlx/option"
)

type (
	Options struct {
		Required         bool
		CheckUnique      bool
		CheckRef         bool
		Location         string
		PresenceProvider *option.PresenceProvider
	}
	Option func(c *Options)
)

func WithPresence() Option {
	return func(c *Options) {
		c.PresenceProvider = &option.PresenceProvider{}
	}
}

//WithUnique with unique option
func WithUnique(flag bool) Option {
	return func(c *Options) {
		c.CheckUnique = flag
	}
}

//WithRef with ref key option
func WithRef(flag bool) Option {
	return func(c *Options) {
		c.CheckRef = flag
	}
}

//WithLocation creates with location option
func WithLocation(location string) Option {
	return func(c *Options) {
		c.Location = location
	}
}

//WithRequired with required optio
func WithRequired(flag bool) Option {
	return func(c *Options) {
		c.Required = flag
	}
}

func NewOptions() *Options {
	return &Options{
		Required:    true,
		CheckUnique: true,
		CheckRef:    true,
	}
}
