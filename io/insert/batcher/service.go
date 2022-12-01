package batcher

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxDurationMs = 40
	defaultMaxElements   = 10000000
	defaultBatchSize     = 100
)

// Service represents batcher service
type Service struct {
	inserter   *insert.Service
	RecordType reflect.Type
	config     *Config
	batch      *Batch
	batchPool  *pool
	mux        sync.Mutex
	ctx        context.Context
	isWatching int32
}

// Config represents batcher's config
type Config struct {
	MaxElements   int
	MaxDurationMs int
	BatchSize     int
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
	sleepTime := time.Millisecond * time.Duration(s.config.MaxDurationMs) / 2
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
		_, _, err := s.inserter.Exec(s.ctx, aBatch.collection.newSlice, option.BatchSize(s.config.BatchSize))
		aBatch.state.err = err
		s.batchPool.Put(aBatch)
	}
}

//New creates a batcher service
func New(ctx context.Context, inserter *insert.Service, rType reflect.Type, config *Config) (*Service, error) {
	if config == nil {
		return nil, fmt.Errorf("batcher's config is nil")
	}
	if inserter == nil {
		return nil, fmt.Errorf("batcher's inserter is nil")
	}
	if rType == nil {
		return nil, fmt.Errorf("batcher's rType is nil")
	}

	if config.MaxDurationMs == 0 {
		config.MaxDurationMs = defaultMaxDurationMs
	}
	if config.MaxElements == 0 {
		config.MaxElements = defaultMaxElements
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}

	provider := func() interface{} {
		return NewBatch(rType, config.MaxElements, config.MaxDurationMs)
	}
	service := &Service{
		inserter:   inserter,
		RecordType: rType,
		batch:      nil,
		config:     config,
		batchPool:  newPool(provider),
		ctx:        ctx,
	}

	return service, nil
}
