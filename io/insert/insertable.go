package insert

import "context"

//Insertable interface to be called on inserting data
type Insertable interface {
	OnInsert(ctx context.Context) error
}
