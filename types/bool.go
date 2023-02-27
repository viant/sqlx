package types

import "fmt"

//Bool customize bool type see https://stackoverflow.com/questions/47535543/mysqls-bit-type-maps-to-which-go-type
type Bool bool

func (b *Bool) Scan(src interface{}) error {
	str, ok := src.(string)
	if !ok {
		return fmt.Errorf("enexpected type for Bool: %T", src)
	}
	switch str {
	case "\x00":
		v := false
		*b = Bool(v)
	case "\x01":
		v := true
		*b = Bool(v)
	}
	return nil
}
