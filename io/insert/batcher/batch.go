package batcher

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// Batch represents batch for collecting data before inserting into db
	Batch struct {
		maxElements int
		started     time.Time
		maxDuration time.Duration
		collection  *Collection
		state       *State
		appendLock  sync.Mutex
		flushed     int32
		pool        *pool
		count       int32
	}

	// State represents state of batch
	State struct {
		err  error
		lock sync.Mutex
	}
)

// Wait returns state's error
func (s *State) Wait() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.err
}

// HasExpired  checks if the batch exceeds its max duration time
func (b *Batch) HasExpired() bool {
	return time.Now().Sub(b.started) > b.maxDuration
}

// TryAcquire checks if batch can be used for collecting data
func (b *Batch) TryAcquire() bool {
	result := int(atomic.AddInt32(&b.count, 1)) <= b.maxElements && time.Now().Sub(b.started) < b.maxDuration
	return result
}

func (b *Batch) init() {
	b.started = time.Now()
	b.state.lock.Lock()
	b.count = 0
	b.flushed = 0
}

func (b *Batch) reset() {
	b.pool = nil
	b.started = time.Now()
	b.collection.Reset()
	b.state.lock.Unlock()
	b.state = &State{}
}

// NewBatch creates a new batch
func NewBatch(rType reflect.Type, maxElements int, maxDurationMs int) *Batch {
	batch := &Batch{}
	batch.maxElements = maxElements
	batch.maxDuration = time.Millisecond * time.Duration(maxDurationMs)
	batch.started = time.Now()
	batch.collection = NewCollection(rType)
	batch.state = &State{}
	return batch
}
