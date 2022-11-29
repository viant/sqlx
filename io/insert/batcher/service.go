package batcher

import (
	"context"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Service represents batcher service
type Service struct {
	inserter   *insert.Service
	config     *config
	batch      *Batch
	batchPool  *pool
	mux        sync.Mutex
	ctx        context.Context
	options    []option.Option
	isWatching int32
}

type config struct {
	recordType    reflect.Type
	maxElements   int
	maxDurationMs int
}

// CanFlush checks possibility of flushing batch
func (s *Batch) CanFlush() bool {
	count := s.collection.Len()
	expectedCount := atomic.LoadInt32(&s.count)
	return count == int(expectedCount)
}

func (s *Service) getActiveBatch() *Batch {
	batch := s.batch
	if batch != nil {
		if batch.TryAcquire() {
			return batch
		}
		go s.tryFlush(batch)
	}
	batch = s.batchPool.Get()
	if atomic.CompareAndSwapInt32(&s.isWatching, 0, 1) {
		go s.monitorBatchExpiry()
	}
	s.batch = batch
	return batch
}

// Collect puts data into a batch
func (s *Service) Collect(recPtr interface{}) (*State, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	batch := s.getActiveBatch()
	batch.collection.Append(recPtr)
	return batch.state, nil
}

func (s *Service) monitorBatchExpiry() {
	sleepTime := time.Millisecond * time.Duration(s.config.maxDurationMs) / 2
	for {
		if s.checkBatchExpiry() {
			return
		}
		time.Sleep(sleepTime)
	}
}

func (s *Service) checkBatchExpiry() bool {
	s.mux.Lock()
	defer func() {
		atomic.StoreInt32(&s.isWatching, 0)
		s.mux.Unlock()
	}()

	batch := s.batch
	if batch == nil {
		return true
	}
	if batch.HasExpired() {
		s.batch = nil
		go s.tryFlush(batch)
		return true
	}
	return false
}

func (s *Service) tryFlush(aBatch *Batch) {
	if atomic.CompareAndSwapInt32(&aBatch.flushed, 0, 1) {
		_, _, err := s.inserter.Exec(s.ctx, aBatch.collection.newSlice, s.options...)
		aBatch.state.err = err
		s.batchPool.Put(aBatch)
	}
}

//New creates a batcher service
func New(ctx context.Context, inserter *insert.Service, rType reflect.Type, maxElements int, maxDurationMs int, options ...option.Option) (*Service, error) {

	provider := func() interface{} {
		return NewBatch(rType, maxElements, maxDurationMs)
	}
	service := &Service{
		inserter: inserter,
		batch:    nil,
		config: &config{
			recordType:    rType,
			maxElements:   maxElements,
			maxDurationMs: maxDurationMs,
		},
		batchPool: newPool(provider),
		ctx:       ctx,
		options:   options,
	}

	return service, nil
}
