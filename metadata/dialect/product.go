package dialect

import (
	"fmt"
	"github.com/viant/sqlx/metadata/dialect/version"
)

//Product represents database product
type Product struct {
	version.Info
	Name string
	Version string
}


//String returns product string
func (p *Product) String() string {
	return fmt.Sprintf("%v:%v", p.Name, p.Version)
}

//NewProduct creates a product
func NewProduct(name, ver string) *Product {
	result := &Product{Name: name, Version: ver}
	if info, err := version.Parse([]byte(name));err == nil {
		result.Info = *info
	}
	return result
}