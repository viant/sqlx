package option

//Args represents prepare statement arguments
type Args struct {
	items []interface{}
}

func (a *Args) Unwrap() []interface{} {
	return a.items
}

//NewArgs creates option arguments
func NewArgs(args ...interface{}) *Args {
	return &Args{args}
}
