package validator

import (
	"github.com/viant/sqlx/option"
	"unsafe"
)

type (
	Options struct {
		CheckNotNull     bool
		CheckUnique      bool
		CheckRef         bool
		PresenceProvider *option.PresenceProvider
	}
	Option func(c *Options)
)

func (o *Options) IsFieldSet(ptr unsafe.Pointer, index int) bool {
	if o.PresenceProvider == nil || o.PresenceProvider.Holder == nil {
		return true //we do not have field presence provider so we assume all fields are set
	}
	return o.PresenceProvider.Has(ptr, index)
}

func WithPresence() Option {
	return func(c *Options) {
		c.PresenceProvider = &option.PresenceProvider{}
	}
}

//option.PresenceProvider{}
func WithUnique(flag bool) Option {
	return func(c *Options) {
		c.CheckUnique = flag
	}
}

func WithRef(flag bool) Option {
	return func(c *Options) {
		c.CheckRef = flag
	}
}

func WithNotNull(flag bool) Option {
	return func(c *Options) {
		c.CheckNotNull = flag
	}
}

func NewOptions() *Options {
	return &Options{
		CheckNotNull: true,
		CheckUnique:  true,
		CheckRef:     true,
	}
}
