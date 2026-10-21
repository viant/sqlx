package ast

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"time"
)

// Database drivers return these named scan types. Keep their exact identities
// in persisted cache metadata; generic nullable unsigned values must not be
// narrowed to signed integers or decoded through floating point.
var sqlScanTypes = []reflect.Type{
	reflect.TypeFor[json.RawMessage](), reflect.TypeFor[sql.RawBytes](),
	reflect.TypeFor[sql.NullBool](), reflect.TypeFor[sql.NullByte](),
	reflect.TypeFor[sql.NullInt16](), reflect.TypeFor[sql.NullInt32](), reflect.TypeFor[sql.NullInt64](),
	reflect.TypeFor[sql.NullFloat64](), reflect.TypeFor[sql.NullString](), reflect.TypeFor[sql.NullTime](),
	reflect.TypeFor[sql.Null[bool]](), reflect.TypeFor[sql.Null[int]](),
	reflect.TypeFor[sql.Null[int8]](), reflect.TypeFor[sql.Null[int16]](), reflect.TypeFor[sql.Null[int32]](), reflect.TypeFor[sql.Null[int64]](),
	reflect.TypeFor[sql.Null[uint]](), reflect.TypeFor[sql.Null[uint8]](), reflect.TypeFor[sql.Null[uint16]](), reflect.TypeFor[sql.Null[uint32]](), reflect.TypeFor[sql.Null[uint64]](),
	reflect.TypeFor[sql.Null[float32]](), reflect.TypeFor[sql.Null[float64]](),
	reflect.TypeFor[sql.Null[string]](), reflect.TypeFor[sql.Null[[]byte]](), reflect.TypeFor[sql.Null[time.Time]](),
}
