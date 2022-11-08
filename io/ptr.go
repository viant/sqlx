package io

import "github.com/viant/xunsafe"

// Int64Ptr returns pointer to index-th element of slice as pointer to int84
func Int64Ptr(values []interface{}, index int) (*int64, error) {
	// TODO serve other types like uint, uint64, int32 ...
	// TODO handling errors
	ptr := xunsafe.AsPointer(values[index])
	return (*int64)(ptr), nil
}
