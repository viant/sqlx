package aerospike

import (
	"sync"
	"time"
)

// FailureHandler temporarily bypasses cache reads after consecutive failures.
// Close stops its reset timer and prevents further timers from being scheduled.
type FailureHandler struct {
	mux        sync.Mutex
	counter    int64
	limit      int64
	resetAfter time.Duration
	timer      *time.Timer
	isProbing  bool
	closed     bool
}

func NewFailureHandler(limit int64, resetAfter *time.Duration) *FailureHandler {
	result := &FailureHandler{limit: limit}
	if resetAfter != nil {
		result.resetAfter = *resetAfter
	}
	return result
}

func (f *FailureHandler) HandleFailure() {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.closed {
		return
	}
	f.counter++
	if f.counter <= f.limit || f.limit == 0 || f.resetAfter <= 0 || f.isProbing {
		return
	}
	f.isProbing = true
	f.timer = time.AfterFunc(f.resetAfter, f.reset)
}

func (f *FailureHandler) reset() {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.closed {
		return
	}
	f.counter = 0
	f.timer = nil
	f.isProbing = false
}

func (f *FailureHandler) HandleSuccess() {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.counter = 0
}

func (f *FailureHandler) Close() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.closed = true
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	return nil
}

func (f *FailureHandler) IsProbing() bool {
	f.mux.Lock()
	defer f.mux.Unlock()
	return f.isProbing
}
