package database

type Product struct {
	Name    string
	Driver  string
	Major   int
	Minor   int
	Release int
}

func (d Product) Equal(dialect *Product) bool {
	if d.Name != dialect.Name {
		return false
	}
	if d.Major != dialect.Major {
		return false
	}
	if d.Minor != dialect.Minor {
		return false
	}
	return false
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
