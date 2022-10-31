package aerospike

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type FailureHandler struct {
	mux        sync.RWMutex
	counter    int64
	limit      int64
	resetAfter time.Duration
	cancelFn   func()
}

func NewFailureHandler(limit int64, resetAfter time.Duration) *FailureHandler {
	return &FailureHandler{
		limit:      limit,
		resetAfter: resetAfter,
	}
}

func (f *FailureHandler) HandleFailure() {
	failed := atomic.AddInt64(&f.counter, 1)
	if failed > f.limit && f.limit != 0 {
		f.startReset()
	}
}

func (f *FailureHandler) Close() error {
	f.mux.Lock()
	if f.cancelFn != nil {
		f.cancelFn()
	}

	f.cancelFn = nil
	f.mux.Unlock()
	return nil
}

func (f *FailureHandler) startReset() {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.cancelFn != nil {
		return
	}

	ctx := context.Background()
	actualCtx, cancelFunc := context.WithCancel(ctx)
	go func() {
		select {
		case <-time.After(f.resetAfter):
			f.mux.Lock()
			atomic.StoreInt64(&f.counter, 0)
			f.cancelFn()
			f.cancelFn = nil
			f.mux.Unlock()
		case <-actualCtx.Done():
			f.mux.Lock()
			f.cancelFn = nil
			f.mux.Unlock()
		}
	}()

	f.cancelFn = cancelFunc
}

func (f *FailureHandler) IsProbing() bool {
	return f.cancelFn != nil
}
