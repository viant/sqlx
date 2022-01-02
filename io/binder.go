package io

//PlaceholderBinder copies source values to params starting with offset
type PlaceholderBinder func(src interface{}, params []interface{}, offset, limit int)
