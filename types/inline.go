package types

import "reflect"

// InlineStruct to inline structs
func InlineStruct(t reflect.Type) reflect.Type {
	rootPkgPath := t.PkgPath()
	return inlineStructRecursive(t, rootPkgPath, map[reflect.Type]reflect.Type{})
}

func inlineStructRecursive(t reflect.Type, rootPkgPath string, cache map[reflect.Type]reflect.Type) reflect.Type {
	// Check if we have already processed this type to prevent infinite recursion
	if cachedType, ok := cache[t]; ok {
		return cachedType
	}

	switch t.Kind() {
	case reflect.Ptr:
		elemType := inlineStructRecursive(t.Elem(), rootPkgPath, cache)
		newType := reflect.PtrTo(elemType)
		cache[t] = newType
		return newType
	case reflect.Slice:
		elemType := inlineStructRecursive(t.Elem(), rootPkgPath, cache)
		newType := reflect.SliceOf(elemType)
		cache[t] = newType
		return newType
	case reflect.Array:
		elemType := inlineStructRecursive(t.Elem(), rootPkgPath, cache)
		newType := reflect.ArrayOf(t.Len(), elemType)
		cache[t] = newType
		return newType
	case reflect.Struct:
		if t.PkgPath() == rootPkgPath && t.Name() != "" {
			// Inline the struct
			fields := make([]reflect.StructField, t.NumField())
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				fieldType := inlineStructRecursive(field.Type, rootPkgPath, cache)
				fields[i] = reflect.StructField{
					Name:      field.Name,
					Type:      fieldType,
					Tag:       field.Tag,
					Anonymous: field.Anonymous,
					PkgPath:   field.PkgPath,
				}
			}
			newType := reflect.StructOf(fields)
			cache[t] = newType
			return newType
		} else {
			// Do not inline, use as is
			cache[t] = t
			return t
		}
	default:
		// Other types
		cache[t] = t
		return t
	}
}
