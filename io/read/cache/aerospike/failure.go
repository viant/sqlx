package aerospike

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type FailureHandler struct {
	mux            sync.RWMutex
	counter        int64
	limit          int64
	resetAfter     time.Duration
	probingResetFn func()
	isProbing      bool
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
		f.startProbing()
	}
}

func (f *FailureHandler) HandleSuccess() {
	atomic.StoreInt64(&f.counter, 0)
}

func (f *FailureHandler) Close() error {
	f.mux.Lock()
	if f.probingResetFn != nil {
		f.probingResetFn()
	}

	f.probingResetFn = nil
	f.mux.Unlock()
	return nil
}

func (f *FailureHandler) startProbing() {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.isProbing {
		return
	}

	f.isProbing = true
	resetFn := f.startTimer(func() {
		f.mux.Lock()
		atomic.StoreInt64(&f.counter, 0)
		f.probingResetFn()
		f.probingResetFn = nil
		f.mux.Unlock()
	})

	f.probingResetFn = resetFn
}

func (f *FailureHandler) startTimer(callback func()) context.CancelFunc {
	ctx := context.Background()
	actualCtx, cancelFunc := context.WithCancel(ctx)

	go func() {
		select {
		case <-time.After(f.resetAfter):
			callback()
		case <-actualCtx.Done():
			//Do nothing
		}
	}()

	return cancelFunc
}

func (f *FailureHandler) IsProbing() bool {
	return f.isProbing
}
