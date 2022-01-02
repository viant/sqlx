package database

//Product represents database product
type Product struct {
	Name      string
	Driver    string
	DriverPkg string
	Major     int
	Minor     int
	Release   int
}

//Equal checks if product are equal
func (p *Product) Equal(dialect *Product) bool {
	if p.Name != dialect.Name {
		return false
	}
	if p.Major != dialect.Major {
		return false
	}
	if p.Minor != dialect.Minor {
		return false
	}
	return false
}

//New crates new product with supplied version
func (p *Product) New(major, minor, release int) *Product {
	return &Product{
		Name:    p.Name,
		Major:   major,
		Minor:   minor,
		Release: release,
	}
}
