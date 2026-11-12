package option

// MetadataPage requests one provider metadata page. Allocate one per operation;
// it must not be shared between concurrent calls. Handled and NextToken are
// outputs, assigned only after a successful provider request. An empty page
// with a NextToken is not the end of the listing.
type MetadataPage struct {
	Limit     int64
	Token     string
	NextToken string
	Handled   bool
}
