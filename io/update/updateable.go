package update

import "context"

//Updatable interface to be called on updating data
type Updatable interface {
	OnUpdate(ctx context.Context) error
}
