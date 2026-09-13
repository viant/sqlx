package cache

import (
	"database/sql"
	"database/sql/driver"
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
