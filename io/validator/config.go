package validator

type (
	Options struct {
		CheckNotNull bool
		CheckUnique  bool
		CheckRef     bool
	}
	Option func(c *Options)
)

func NewOptions() *Options {
	return &Options{
		CheckNotNull: true,
		CheckUnique:  true,
		CheckRef:     true,
	}
}
