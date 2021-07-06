package database

type Product struct {
	Name    string
	Driver  string
	Major   int
	Minor   int
	Release int
}

//New crates new product with supplied version
func (p Product) New(major, minor, release int) *Product {
	return &Product{
		Name:    p.Name,
		Major:   major,
		Minor:   minor,
		Release: release,
	}
}
