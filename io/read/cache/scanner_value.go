package cache

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
)

// scannerValue preserves the destination held by Scanner/Valuer adapters on
// replay, rather than copying over their pointers into the actual model.
func scannerValue(destination, cached any) (bool, error) {
	scanner, ok := destination.(sql.Scanner)
	if !ok {
		return false, nil
	}
	if cached == nil {
		return true, scanner.Scan(nil)
	}
	to, from := reflect.ValueOf(destination), reflect.ValueOf(cached)
	if to.Kind() == reflect.Pointer && from.Kind() == reflect.Pointer && !to.IsNil() && !from.IsNil() && to.Type() == from.Type() && isSQLNullType(to.Elem().Type()) {
		// sql.Null[uint64].Value rejects values above MaxInt64; replaying an
		// already typed nullable value is assignment, not driver conversion.
		to.Elem().Set(from.Elem())
		return true, nil
	}
	valuer, ok := cached.(driver.Valuer)
	if !ok {
		return false, nil
	}
	raw, err := valuer.Value()
	if err != nil {
		return true, err
	}
	return true, scanner.Scan(raw)
}
