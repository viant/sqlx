package cache

type Meta struct {
	SQL          string
	Args         []byte
	Type         []string
	Signature    string
	ExpiryTimeMs int
	Fields       []*Field

	URL string `json:"-" yaml:"-"`
}
