package ast

import (
	"reflect"
	"time"
)

var IntType = reflect.TypeOf(0)
var Int8Type = reflect.TypeOf(int8(0))
var Int16Type = reflect.TypeOf(int16(0))
var Int32Type = reflect.TypeOf(int32(0))
var Int64Type = reflect.TypeOf(int64(0))

var UintType = reflect.TypeOf(uint(0))
var Uint8Type = reflect.TypeOf(uint8(0))
var Uint16Type = reflect.TypeOf(uint16(0))
var Uint32Type = reflect.TypeOf(uint32(0))
var Uint64Type = reflect.TypeOf(uint64(0))

var Float32Type = reflect.TypeOf(float32(0.0))
var Float64Type = reflect.TypeOf(0.0)

var StringType = reflect.TypeOf("")
var BoolType = reflect.TypeOf(false)

var TimeType = reflect.TypeOf(time.Time{})
