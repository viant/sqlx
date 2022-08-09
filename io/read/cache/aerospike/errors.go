package aerospike

import "sync"

type Errors struct {
	errors []error
	sync.Mutex
}

func (e *Errors) Add(err error) {
	if err == nil {
		return
	}
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	e.errors = append(e.errors, err)
}

func (e *Errors) Err() error {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	if len(e.errors) == 0 {
		return nil
	}
	return e.errors[0]
}
