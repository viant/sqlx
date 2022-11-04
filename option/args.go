package option

import "fmt"

//Args represents prepare statement arguments
type Args struct {
	items []interface{}
}

func (a *Args) Unwrap() []interface{} {
	return a.items
}

func (a *Args) StringN(n int) ([]string, error) {
	if len(a.items) < 3 {
		return nil, fmt.Errorf("expected %v, but had: %v", n, len(a.items))
	}
	var result = make([]string, n)
	var ok bool
	for i := 0; i < len(a.items); i++ {
		result[i], ok = a.items[i].(string)
		if !ok {
			return nil, fmt.Errorf("expected %T, but had: %T at %v", result[i], a.items[i], i)
		}
	}
	return result, nil
}

//NewArgs creates option arguments
func NewArgs(args ...interface{}) *Args {
	return &Args{args}
}
