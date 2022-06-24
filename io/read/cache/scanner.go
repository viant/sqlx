package cache

import (
	"encoding/json"
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"time"
	"unsafe"
)

type (
	Scanner struct {
		scan scannerFn
	}

	scannerFn func(dest, val interface{}) error
)

func NewScanner(dest interface{}) *Scanner {
	return &Scanner{scan: newScannerFn(dest)}
}

func (s *Scanner) Scan(dest, value interface{}) error {
	if value == nil {
		return nil
	}

	return s.scan(dest, value)
}

func newScannerFn(dest interface{}) scannerFn {
	switch actual := dest.(type) {
	case *int:
		return intPtrScanner(actual)
	case **int:
		return intPtrToPtrScanner(actual)
	case *int8:
		return int8PtrScanner(actual)
	case **int8:
		return int8PtrToPtrScanner(actual)
	case *int16:
		return int16PtrScanner(actual)
	case **int16:
		return int16PtrToPtrScanner(actual)
	case *int32:
		return int32PtrScanner(actual)
	case **int32:
		return int32PtrToPtrScanner(actual)
	case *int64:
		return int64PtrScanner(actual)
	case **int64:
		return int64PtrToPtrScanner(actual)
	case *uint:
		return uintPtrScanner(actual)
	case **uint:
		return uintPtrToPtrScanner(actual)
	case *uint8:
		return uint8PtrScanner(actual)
	case **uint8:
		return uint8PtrToPtrScanner(actual)
	case *uint16:
		return uint16PtrScanner(actual)
	case **uint16:
		return uint16PtrToPtrScanner(actual)
	case *uint32:
		return uint32PtrScanner(actual)
	case **uint32:
		return uint32PtrToPtrScanner(actual)
	case *uint64:
		return uint64PtrScanner(actual)
	case **uint64:
		return uint64PtrToPtrScanner(actual)
	case *string:
		return stringPtrScanner(actual)
	case **string:
		return stringPtrToPtrScanner(actual)
	case *float32:
		return float32PtrScanner(actual)
	case **float32:
		return float32PtrToPtrScanner(actual)
	case *float64:
		return float64PtrScanner(actual)
	case **float64:
		return float64PtrToPtrScanner(actual)
	case *bool:
		return boolPtrScanner(actual)
	case **bool:
		return boolPtrToPtrScanner(actual)
	case *time.Time:
		return timePtrScanner(actual)
	case **time.Time:
		return timePtrToPtrScanner(actual)
	}

	//TODO: need to optimize
	return jsonScanner(dest)
}

func jsonScanner(actual interface{}) scannerFn {
	actualType := reflect.TypeOf(actual)
	return func(dest, val interface{}) error {
		asBytes, err := json.Marshal(val)
		if err != nil {
			return err
		}

		value := reflect.New(actualType)
		if err = json.Unmarshal(asBytes, value.Interface()); err != nil {
			return err
		}

		if value.IsNil() {
			return nil
		}

		xunsafe.Copy(xunsafe.AsPointer(dest), unsafe.Pointer(value.Pointer()), int(actualType.Size()))
		return nil
	}
}

func timePtrToPtrScanner(actual **time.Time) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**time.Time)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(string)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		aTime, err := time.Parse(time.RFC3339, actualVal)
		if err != nil {
			return err
		}

		*actualDest = &aTime
		return nil
	}

}

func boolPtrToPtrScanner(actual **bool) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**bool)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(bool)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = &actualVal
		return nil
	}

}

func timePtrScanner(actual *time.Time) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*time.Time)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(string)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		aTime, err := time.Parse(time.RFC3339, actualVal)
		if err != nil {
			return err
		}

		*actualDest = aTime
		return nil
	}

}

func boolPtrScanner(actual *bool) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*bool)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(bool)

		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = actualVal
		return nil
	}

}

func float64PtrToPtrScanner(actual **float64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**float64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(float64)

		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = &actualVal
		return nil
	}
}

func float64PtrScanner(actual *float64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*float64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(float64)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = actualVal
		return nil
	}

}

func float32PtrToPtrScanner(actual **float32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**float32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(float64)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		asFloat32 := float32(actualVal)
		*actualDest = &asFloat32
		return nil
	}
}

func float32PtrScanner(actual *float32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*float32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(float64)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = float32(actualVal)
		return nil
	}
}

func stringPtrToPtrScanner(actual **string) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**string)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(string)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = &actualVal
		return nil
	}
}

func stringPtrScanner(actual *string) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*string)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := val.(string)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = actualVal
		return nil
	}
}

func uint64PtrToPtrScanner(actual **uint64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**uint64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := uint64(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func uint32PtrToPtrScanner(actual **uint32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**uint32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := uint32(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func uint16PtrToPtrScanner(actual **uint16) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**uint16)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := uint16(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func uint8PtrToPtrScanner(actual **uint8) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**uint8)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := uint8(actualVal)
		*actualDest = &actualConverted
		return nil
	}

}

func uintPtrToPtrScanner(actual **uint) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**uint)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := uint(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func uint64PtrScanner(actual *uint64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*uint64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = uint64(actualVal)
		return nil
	}

}

func uint32PtrScanner(actual *uint32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*uint32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = uint32(actualVal)
		return nil
	}
}

func uint16PtrScanner(actual *uint16) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*uint16)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = uint16(actualVal)
		return nil
	}
}

func uint8PtrScanner(actual *uint8) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*uint8)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = uint8(actualVal)
		return nil
	}
}

func uintPtrScanner(actual *uint) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*uint)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = uint(actualVal)
		return nil
	}
}

func int64PtrToPtrScanner(actual **int64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**int64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		*actualDest = &actualVal
		return nil
	}
}

func int64PtrScanner(actual *int64) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*int64)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = actualVal
		return nil
	}
}

func int32PtrToPtrScanner(actual **int32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**int32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := int32(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func int32PtrScanner(actual *int32) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*int32)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = int32(actualVal)
		return nil
	}

}

func intPtrToPtrScanner(actual **int) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**int)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := int(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func intPtrScanner(actual interface{}) func(dest interface{}, val interface{}) error {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*int)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = int(actualVal)
		return nil
	}
}

func int8PtrScanner(actual *int8) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*int8)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = int8(actualVal)
		return nil
	}
}

func int8PtrToPtrScanner(actual **int8) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**int8)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := int8(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func int16PtrScanner(actual *int16) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(*int16)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}

		*actualDest = int16(actualVal)
		return nil
	}
}

func int16PtrToPtrScanner(actual **int16) scannerFn {
	return func(dest, val interface{}) error {
		actualDest, ok := dest.(**int16)
		if !ok {
			return newTypeMissmatchError(actual, dest)
		}
		actualVal, ok := normalizeInt(val)
		if !ok {
			return newTypeMissmatchError(actualVal, val)
		}
		actualConverted := int16(actualVal)
		*actualDest = &actualConverted
		return nil
	}
}

func newTypeMissmatchError(wanted, got interface{}) error {
	return fmt.Errorf("type missmatch, wanted %T, got %T", wanted, got)
}
