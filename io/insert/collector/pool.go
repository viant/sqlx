package collector

import (
	"sync"
)

type pool struct {
	sync.Pool
}

// Get returns an item from the pool or a new item (if the pool is empty)
func (p *pool) Get() *Batch {
	batch, _ := p.Pool.Get().(*Batch)
	batch.pool = p
	batch.init()
	return batch
}

// Put puts an item into the pool
func (p *pool) Put(batch *Batch) {
	if batch.pool == nil {
		return
	}
	batch.reset()
	p.Pool.Put(batch)
}

func newPool(provider func() interface{}) *pool {
	return &pool{Pool: sync.Pool{New: provider}}
}
