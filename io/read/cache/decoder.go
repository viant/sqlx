package cache

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"time"
)

var timeType = reflect.TypeOf(time.Time{})

type (
	Decoder struct {
		scanTypes []reflect.Type
		decoders  []DecoderFn
		values    []interface{}
		index     int

		sliceType    reflect.Type
		sliceDecoder DecoderFn
	}

	DecoderFn func(decoder *gojay.Decoder) (interface{}, error)
)

func NewDecoder(scanTypes []reflect.Type) *Decoder {
	return &Decoder{
		scanTypes: scanTypes,
		values:    make([]interface{}, len(scanTypes)),
	}
}

func (d *Decoder) UnmarshalJSONArray(decoder *gojay.Decoder) error {
	d.buildDecoders()
	index := d.index
	if index > len(d.values)-1 && len(d.scanTypes) != 0 {
		return fmt.Errorf("unexpected value, expected to got %v values", len(d.values))
	}

	d.index++

	var decoderFn DecoderFn
	if len(d.scanTypes) > 0 {
		decoderFn = d.decoders[index]
	} else {
		decoderFn = d.sliceDecoder
	}

	value, err := decoderFn(decoder)
	if err != nil {
		return err
	}

	if len(d.scanTypes) > 0 {
		d.values[index] = value
	} else {
		d.values = append(d.values, value)
	}
	return nil
}

func (d *Decoder) buildDecoders() {
	if len(d.decoders) > 0 {
		return
	}

	d.decoders = make([]DecoderFn, len(d.scanTypes))
	for i, dataType := range d.scanTypes {
		d.decoders[i] = newDecoderFn(dataType)
	}
}

func (d *Decoder) reset() {
	for i := range d.values {
		d.values[i] = nil
	}
	d.index = 0
}

func newDecoderFn(dataType reflect.Type) DecoderFn {
	actualDataType := dataType

	wasPtr := false
	if dataType.Kind() == reflect.Ptr {
		wasPtr = true
		dataType = dataType.Elem()
	}

	switch dataType.Kind() {
	case reflect.Int:
		return intDecoder(wasPtr)
	case reflect.Int8:
		return int8Decoder(wasPtr)
	case reflect.Int16:
		return int16Decoder(wasPtr)
	case reflect.Int32:
		return int32Decoder(wasPtr)
	case reflect.Int64:
		return int64Decoder(wasPtr)
	case reflect.Uint8:
		return uint8Decoder(wasPtr)
	case reflect.Uint16:
		return uint16Decoder(wasPtr)
	case reflect.Uint32:
		return uint32Decoder(wasPtr)
	case reflect.Uint64:
		return uint64Decoder(wasPtr)
	case reflect.Float32:
		return float32Decoder(wasPtr)
	case reflect.Float64:
		return float64Decoder(wasPtr)
	case reflect.String:
		return stringDecoder(wasPtr)
	case reflect.Slice:
		sliceItemType := dataType.Elem()
		xType := xunsafe.NewType(sliceItemType)
		return func(decoder *gojay.Decoder) (interface{}, error) {
			valuesDecoder := &Decoder{
				sliceType:    sliceItemType,
				sliceDecoder: newDecoderFn(sliceItemType),
			}

			if err := decoder.DecodeArray(valuesDecoder); err != nil {
				return nil, err
			}

			for i, value := range valuesDecoder.values {
				valuesDecoder.values[i] = xType.Deref(value)
			}

			return &valuesDecoder.values, nil
		}

	case reflect.Bool:
		return boolDecoder(wasPtr)
	}

	return interfaceDecoder(actualDataType)
}

//func timeDecoder(ptr bool) DecoderFn {
//	return func(decoder *gojay.Decoder) (interface{}, error) {
//		aTime := time.Time{}
//
//		return &aTime, decoder.Time(&aTime, time.RFC3339Nano)
//	}
//}

func interfaceDecoder(actualDataType reflect.Type) DecoderFn {
	return func(decoder *gojay.Decoder) (interface{}, error) {
		rValue := reflect.New(actualDataType)
		asInterface := rValue.Interface()

		return asInterface, decoder.Interface(&asInterface)
	}
}

func boolDecoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aBool := false
			return &aBool, decoder.Bool(&aBool)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		aBoolPtr := new(bool)
		return &aBoolPtr, decoder.BoolNull(&aBoolPtr)
	}

}

func stringDecoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aString := ""
			return &aString, decoder.String(&aString)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		aStringPtr := new(string)
		return &aStringPtr, decoder.StringNull(&aStringPtr)
	}
}

func float64Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aFloat := float64(0)
			return &aFloat, decoder.Float64(&aFloat)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		floatPtr := new(float64)
		return &floatPtr, decoder.Float64Null(&floatPtr)
	}
}

func float32Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := float32(0)
			return &anInt, decoder.Float32(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		float32Ptr := new(float32)
		return &float32Ptr, decoder.Float32Null(&float32Ptr)
	}
}

func uint64Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anUint64 := uint64(0)
			return &anUint64, decoder.Uint64(&anUint64)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		uint64Ptr := new(uint64)
		return &uint64Ptr, decoder.Uint64Null(&uint64Ptr)
	}
}

func uint32Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anUint32 := uint32(0)
			return &anUint32, decoder.Uint32(&anUint32)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(uint32)
		return &anIntPtr, decoder.Uint32Null(&anIntPtr)
	}
}

func uint16Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := uint16(0)
			return &anInt, decoder.Uint16(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(uint16)
		return &anIntPtr, decoder.Uint16Null(&anIntPtr)
	}
}

func uint8Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := uint8(0)
			return &anInt, decoder.Uint8(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(uint8)
		return &anIntPtr, decoder.Uint8Null(&anIntPtr)
	}
}

func int64Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := int64(0)
			return &anInt, decoder.Int64(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(int64)
		return &anIntPtr, decoder.Int64Null(&anIntPtr)
	}
}

func int32Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := int32(0)
			return &anInt, decoder.Int32(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(int32)
		return &anIntPtr, decoder.Int32Null(&anIntPtr)
	}
}

func int16Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := int16(0)
			return &anInt, decoder.Int16(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(int16)
		return &anIntPtr, decoder.Int16Null(&anIntPtr)
	}
}

func int8Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := int8(0)
			return &anInt, decoder.Int8(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(int8)
		return &anIntPtr, decoder.Int8Null(&anIntPtr)
	}
}

func intDecoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			anInt := 0
			return &anInt, decoder.Int(&anInt)
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		anIntPtr := new(int)
		return &anIntPtr, decoder.IntNull(&anIntPtr)
	}
}
