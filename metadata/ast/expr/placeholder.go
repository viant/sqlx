package expr

type Placeholder struct {
	Name string
}

func NewPlaceholder(name string) *Placeholder {
	return &Placeholder{Name: name}
}
