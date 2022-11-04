package io

import "github.com/viant/xunsafe"

//TODO COMMENT
// Int64Ptr returns p
func Int64Ptr(values []interface{}, index int) (*int64, error) {
	// TODO WHAT WITH OTHER TYPES LIKE uint, uint64, int32 ...
	// TODO fmt.Errorf("expected *int or *int64 for identity, got %T", idPtr)
	ptr := xunsafe.AsPointer(values[index])
	return (*int64)(ptr), nil
}
