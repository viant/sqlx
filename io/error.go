package io

// RunWithError sets err as error from passed function
// used i.e. with deffer
func RunWithError(fn func() error, err *error) {
	currErr := fn()
	if currErr == nil || *err != nil {
		return
	}
	*err = currErr
}
