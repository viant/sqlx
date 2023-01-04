package batcher

import (
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

// Collection represents slice to collecting data
type Collection struct {
	xSlice   *xunsafe.Slice
	newSlice interface{}
	slicePtr unsafe.Pointer
	appender *xunsafe.Appender
	RWMutex  sync.RWMutex
}

// ValuePointerAt returns item at given index
func (c *Collection) ValuePointerAt(index int) interface{} {
	return c.xSlice.ValuePointerAt(c.slicePtr, index)
}

func (c *Collection) Unwrap() interface{} {
	return c.newSlice
}

// Len returns count of items in collection
func (c *Collection) Len() int {
	c.RWMutex.RLock()
	result := c.xSlice.Len(c.slicePtr)
	c.RWMutex.RUnlock()
	return result
}

// Append adds item into collection
func (c *Collection) Append(value interface{}) {
	c.RWMutex.Lock()
	c.appender.Append(value)
	c.RWMutex.Unlock()
}

// Reset set length collection to 0 (doesn't change capacity)
func (c *Collection) Reset() {
	c.RWMutex.Lock()
	_ = c.appender.Trunc(0)
	c.RWMutex.Unlock()
}

// NewCollection creates a new collection
func NewCollection(rType reflect.Type) *Collection {
	xSlice := getXSlice(rType)
	newSlice := getNewSlice(rType)
	slicePtr := xunsafe.AsPointer(newSlice)
	appender := xSlice.Appender(slicePtr)

	collection := &Collection{
		xSlice:   xSlice,
		newSlice: newSlice,
		appender: appender,
		slicePtr: slicePtr,
		RWMutex:  sync.RWMutex{},
	}
	return collection
}

func getNewSlice(rType reflect.Type) interface{} {
	sliceType := reflect.SliceOf(rType)
	newSliceValue := reflect.New(sliceType)
	newSlice := newSliceValue.Interface()
	return newSlice
}

func getXSlice(rType reflect.Type) *xunsafe.Slice {
	sliceType := reflect.SliceOf(rType)
	return xunsafe.NewSlice(sliceType)
}
