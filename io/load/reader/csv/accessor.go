package csv

import (
	"github.com/viant/xunsafe"
	"unsafe"
)

type (
	Accessor struct {
		_parent             *Accessor
		cache               map[unsafe.Pointer]*stringified
		emitedFirst         bool
		parentAccessorIndex int
		fields              []*Field // requested fields
		children            []*Accessor
		config              *Config
		path                string

		currSliceIndex int
		slicePtr       unsafe.Pointer
		slice          *xunsafe.Slice

		ptr   unsafe.Pointer // refering to a single object
		field *xunsafe.Field // used to get value from parent pointer
	}

	stringified struct {
		values     []string
		wasStrings []bool
	}
)

func (a *Accessor) Reset() {
	a.Set(nil)
	a.emitedFirst = false
	a.cache = map[unsafe.Pointer]*stringified{}
}

func (a *Accessor) Has() bool {
	if a.emitedFirst {
		return a.prepare()
	}

	a.emitedFirst = true
	return a.ptr != nil
}

func (a *Accessor) prepare() bool {
	accessor, ok := a.next()
	if !ok {
		return false
	}

	parent, childIndex := sliceParentOf(accessor)
	for i := 0; i < childIndex; i++ {
		parent.children[i].Set(parent.ptr)
	}

	return true
}

func sliceParentOf(accessor *Accessor) (*Accessor, int) {
	for accessor != nil {
		if accessor._parent != nil && accessor._parent.slice != nil {
			return accessor._parent, accessor.parentAccessorIndex
		}

		accessor = accessor._parent
	}

	return nil, -1
}

func (a *Accessor) Set(pointer unsafe.Pointer) {
	a.ptr = pointer

	for _, child := range a.children {
		if pointer == nil {
			child.Set(nil)
		} else {
			aPtr, slicePtr := a.getChildValue(pointer, child)
			child.slicePtr = slicePtr
			child.Set(aPtr)
		}

		child.currSliceIndex = 0
	}
}

func (a *Accessor) getChildValue(pointer unsafe.Pointer, child *Accessor) (valuePtr unsafe.Pointer, slicePtr unsafe.Pointer) {
	valuePointer := child.field.ValuePointer(pointer)
	if child.slice == nil {
		return valuePointer, nil
	}

	lenSlice := child.slice.Len(valuePointer)
	if lenSlice == 0 {
		return nil, valuePointer
	}

	at := child.slice.ValuePointerAt(valuePointer, 0)
	return xunsafe.AsPointer(at), valuePointer
}

func (a *Accessor) Stringify() ([]string, []bool) {
	result, wasStrings := a.stringifyFields()
	for _, child := range a.children {
		childValues, childWasStrings := child.Stringify()
		result = append(result, childValues...)
		wasStrings = append(wasStrings, childWasStrings...)
	}

	return result, wasStrings
}

func (a *Accessor) Headers() ([]string, []bool) {
	headers := make([]string, 0, len(a.fields))
	wasStrings := make([]bool, 0, len(a.fields))
	for _, field := range a.fields {
		headers = append(headers, field.header)
		wasStrings = append(wasStrings, true)
	}

	for _, child := range a.children {
		childHeaders, childWasStrings := child.Headers()
		headers = append(headers, childHeaders...)
		wasStrings = append(wasStrings, childWasStrings...)
	}

	return headers, wasStrings
}

func (a *Accessor) stringifyFields() ([]string, []bool) {
	if value, ok := a.cache[a.ptr]; ok {
		return value.values, value.wasStrings
	}

	if a.ptr == nil {
		strings := make([]string, len(a.fields))
		for i := range strings {
			strings[i] = a.config.NullValue
		}

		return strings, make([]bool, len(a.fields))
	}

	result := make([]string, len(a.fields))
	wasStrings := make([]bool, len(a.fields))
	for i, field := range a.fields {
		result[i], wasStrings[i] = field.stringifier(a.ptr)
	}

	a.cache[a.ptr] = &stringified{
		values:     result,
		wasStrings: wasStrings,
	}

	return result, wasStrings
}

//next returns true if record was not exhausted and first not ehausted Accessor
func (a *Accessor) next() (*Accessor, bool) {
	for _, child := range a.children {
		if accessor, ok := child.next(); ok {
			return accessor, true
		}
	}

	if a.slice != nil && a.slicePtr != nil {
		sliceLen := a.slice.Len(a.slicePtr)
		if a.currSliceIndex < sliceLen-1 {
			a.currSliceIndex++
			value := a.slice.ValuePointerAt(a.slicePtr, a.currSliceIndex)
			a.Set(xunsafe.AsPointer(value))
			return a, true
		}

		a.currSliceIndex = 0
	}

	return nil, false
}
