package base

import (
	"errors"
	"fmt"
)

type unsupported struct {
	Driver string
	Operation string
}

//Error return error
func (u *unsupported) Error() string {
	return fmt.Sprintf("unsupported " + u.Operation + " on " + u.Driver)
}

//NewUnsupported creates unsupported error
func NewUnsupported(driver, operation string) error {
	return &unsupported{Driver: driver, Operation: operation}
}


//IsUnsupported returns true if unsupported
func IsUnsupported(err error) bool {
	if err == nil {
		return false
	}
	return 	errors.Is(err, &unsupported{})
}
