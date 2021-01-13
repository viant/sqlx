package metadata

import (
	"sync"
)


var _registry = &registry{}


type registry struct {
	mux sync.Mutex
	//drivers map[string]ReporterProvider
}

//
//func (r *registry) Register(name string, reporterProvider ReporterProvider) error {
//	r.mux.Lock()
//	defer r.mux.Unlock()
//	r.drivers[name] = reporterProvider
//	return nil
//}


