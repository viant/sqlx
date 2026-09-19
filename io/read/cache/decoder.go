package cache

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

var timeType = reflect.TypeOf(time.Time{})
var rawBytesType = reflect.TypeOf(sql.RawBytes(nil))
var curOffset uintptr
var dataOffset uintptr

func init() {
	cur, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("cursor")
	if !ok {
		panic("failed to get Decoder.cursor field")
	}
	curOffset = cur.Offset
	data, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("data")
	if !ok {
		panic("failed to get Decoder.data field")
	}
	dataOffset = data.Offset
}

func cursor(dec *gojay.Decoder) int {
	return *(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(dec)) + curOffset))
}

func data(dec *gojay.Decoder) []byte {
	return *(*[]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(dec)) + dataOffset))
}

func setCursor(dec *gojay.Decoder, value int) {
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(dec)) + curOffset)) = value
}

type (
	Decoder struct {
		scanTypes []reflect.Type
		decoders  []DecoderFn
		values    []interface{}
		index     int

		sliceType    reflect.Type
		sliceDecoder DecoderFn
		Data         []byte
	}

	DecoderFn func(decoder *gojay.Decoder) (interface{}, error)
)

func NewDecoder(scanTypes []reflect.Type, data []byte) *Decoder {
	return &Decoder{
		scanTypes: scanTypes,
		values:    make([]interface{}, len(scanTypes)),
		Data:      data,
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

	wasNull := nextTokenNull(decoder)
	value, err := decoderFn(decoder)
	if err != nil {
		return err
	}
	if wasNull {
		value = nil
	}

	if len(d.scanTypes) > 0 {
		d.values[index] = value
	} else {
		d.values = append(d.values, value)
	}
	return nil
}

func nextTokenNull(decoder *gojay.Decoder) bool {
	buffer := data(decoder)
	position, _ := nextTokenPosition(buffer, cursor(decoder))
	if position < 0 {
		return false
	}
	if position+4 > len(buffer) {
		return false
	}
	if buffer[position] != 'n' || buffer[position+1] != 'u' || buffer[position+2] != 'l' || buffer[position+3] != 'l' {
		return false
	}
	if position+4 == len(buffer) {
		return true
	}
	switch buffer[position+4] {
	case ' ', '\n', '\t', '\r', ',', ']', '}':
		return true
	default:
		return false
	}
}

func nextTokenPosition(buffer []byte, position int) (int, bool) {
	for position < len(buffer) {
		switch buffer[position] {
		case ' ', '\n', '\t', '\r', ',':
			position++
			continue
		}
		break
	}
	if position >= len(buffer) {
		return 0, false
	}
	return position, true
}

func consumeNullToken(decoder *gojay.Decoder) bool {
	buffer := data(decoder)
	position, ok := nextTokenPosition(buffer, cursor(decoder))
	if !ok || position+4 > len(buffer) {
		return false
	}
	if string(buffer[position:position+4]) != "null" {
		return false
	}
	if position+4 < len(buffer) {
		switch buffer[position+4] {
		case ' ', '\n', '\t', '\r', ',', ']', '}':
		default:
			return false
		}
	}
	setCursor(decoder, position+4)
	return true
}

func nextFloatToken(decoder *gojay.Decoder) (string, error) {
	buffer := data(decoder)
	position, ok := nextTokenPosition(buffer, cursor(decoder))
	if !ok {
		return "", fmt.Errorf("unexpected end of JSON while decoding float")
	}
	start := position
	if buffer[position] == '-' {
		position++
		if position >= len(buffer) {
			return "", fmt.Errorf("unexpected end of JSON while decoding float")
		}
	}
	digits := 0
	for position < len(buffer) && isJSONDigit(buffer[position]) {
		position++
		digits++
	}
	if position < len(buffer) && buffer[position] == '.' {
		position++
		for position < len(buffer) && isJSONDigit(buffer[position]) {
			position++
			digits++
		}
	}
	if digits == 0 {
		return "", fmt.Errorf("invalid JSON float token")
	}
	if position < len(buffer) && (buffer[position] == 'e' || buffer[position] == 'E') {
		position++
		if position < len(buffer) && (buffer[position] == '+' || buffer[position] == '-') {
			position++
		}
		expDigits := 0
		for position < len(buffer) && isJSONDigit(buffer[position]) {
			position++
			expDigits++
		}
		if expDigits == 0 {
			return "", fmt.Errorf("invalid JSON float exponent")
		}
	}
	end := position
	if end < len(buffer) {
		switch buffer[end] {
		case ' ', '\n', '\t', '\r', ',', ']', '}':
		default:
			return "", fmt.Errorf("invalid JSON float delimiter")
		}
	}
	setCursor(decoder, end)
	return string(buffer[start:end]), nil
}

func isJSONDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func decodeFloat64Value(decoder *gojay.Decoder) (float64, bool, error) {
	if consumeNullToken(decoder) {
		return 0, true, nil
	}
	token, err := nextFloatToken(decoder)
	if err != nil {
		return 0, false, err
	}
	parsed, err := strconv.ParseFloat(token, 64)
	if err != nil {
		return 0, false, err
	}
	return parsed, false, nil
}

func decodeFloat32Value(decoder *gojay.Decoder) (float32, bool, error) {
	if consumeNullToken(decoder) {
		return 0, true, nil
	}
	token, err := nextFloatToken(decoder)
	if err != nil {
		return 0, false, err
	}
	parsed, err := strconv.ParseFloat(token, 32)
	if err != nil {
		return 0, false, err
	}
	return float32(parsed), false, nil
}

func (d *Decoder) buildDecoders() {
	if len(d.decoders) > 0 {
		return
	}

	d.decoders = make([]DecoderFn, len(d.scanTypes))
	for i, dataType := range d.scanTypes {
		d.decoders[i] = newDecoderFn(dataType, d.Data)
	}
}

func (d *Decoder) reset() {
	for i := range d.values {
		d.values[i] = nil
	}
	d.index = 0
}

func newDecoderFn(dataType reflect.Type, data []byte) DecoderFn {
	actualDataType := dataType

	wasPtr := false
	for dataType.Kind() == reflect.Ptr {
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
		if isByteSliceType(dataType) {
			return bytesDecoder(actualDataType)
		}
		sliceItemType := dataType.Elem()
		xType := xunsafe.NewType(sliceItemType)
		return func(decoder *gojay.Decoder) (interface{}, error) {
			valuesDecoder := &Decoder{
				sliceType:    sliceItemType,
				sliceDecoder: newDecoderFn(sliceItemType, data),
				Data:         data,
			}

			if err := decoder.DecodeArray(valuesDecoder); err != nil {
				return nil, err
			}

			typedValues := reflect.MakeSlice(dataType, len(valuesDecoder.values), len(valuesDecoder.values))
			for i, value := range valuesDecoder.values {
				if value == nil {
					if err := assignDecodedSliceNull(typedValues.Index(i)); err != nil {
						return nil, err
					}
					continue
				}
				if err := assignDecodedSliceValue(typedValues.Index(i), xType.Deref(value)); err != nil {
					return nil, err
				}
			}

			return wrapDecodedValue(actualDataType, typedValues), nil
		}

	case reflect.Bool:
		return boolDecoder(wasPtr)
	default:
		if dataType == timeType {
			return timeDecoder(wasPtr, actualDataType)
		}
	}

	return interfaceDecoder(actualDataType)
}

func bytesDecoder(actualDataType reflect.Type) DecoderFn {
	return func(decoder *gojay.Decoder) (interface{}, error) {
		encoded := ""
		if err := decoder.String(&encoded); err != nil {
			return nil, err
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		if actualDataType == rawBytesType {
			value := sql.RawBytes(decoded)
			return &value, nil
		}
		value := []byte(decoded)
		return &value, nil
	}
}

func timeDecoder(ptr bool, actualDataType reflect.Type) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aTime := time.Time{}
			return &aTime, decoder.Time(&aTime, time.RFC3339Nano)
		}
	}

	return interfaceDecoder(actualDataType)
}

func interfaceDecoder(actualDataType reflect.Type) DecoderFn {
	return func(decoder *gojay.Decoder) (interface{}, error) {
		rValue := reflect.New(actualDataType)
		asInterface := rValue.Interface()

		return asInterface, decoder.Interface(&asInterface)
	}
}

func wrapDecodedValue(actualDataType reflect.Type, value reflect.Value) interface{} {
	container := reflect.New(actualDataType)
	assignWrappedValue(container.Elem(), value)
	return container.Interface()
}

func assignWrappedValue(target reflect.Value, value reflect.Value) {
	if target.Kind() == reflect.Ptr {
		nested := reflect.New(target.Type().Elem())
		assignWrappedValue(nested.Elem(), value)
		target.Set(nested)
		return
	}
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return
	}
	target.Set(value.Convert(target.Type()))
}

func assignDecodedSliceNull(target reflect.Value) error {
	switch target.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice:
		target.Set(reflect.Zero(target.Type()))
		return nil
	default:
		return fmt.Errorf("Cannot unmarshal JSON to type '%s'", target.Type().String())
	}
}

func assignDecodedSliceValue(target reflect.Value, value interface{}) error {
	if value == nil {
		return assignDecodedSliceNull(target)
	}
	source := reflect.ValueOf(value)
	if !source.IsValid() {
		return assignDecodedSliceNull(target)
	}
	if source.Type().AssignableTo(target.Type()) {
		target.Set(source)
		return nil
	}
	if source.Type().ConvertibleTo(target.Type()) {
		target.Set(source.Convert(target.Type()))
		return nil
	}
	if target.Kind() == reflect.Interface && source.Type().AssignableTo(target.Type()) {
		target.Set(source)
		return nil
	}
	return fmt.Errorf("Cannot unmarshal JSON to type '%s'", target.Type().String())
}

func isByteSliceType(rType reflect.Type) bool {
	return rType != nil && rType.Kind() == reflect.Slice && rType.Elem().Kind() == reflect.Uint8
}

func boolDecoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aBool, wasNull, err := decodeBoolLike(decoder)
			if err != nil {
				return nil, err
			}
			if wasNull {
				return nil, fmt.Errorf("Cannot unmarshal JSON to type 'bool'")
			}
			return &aBool, nil
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		aBool, wasNull, err := decodeBoolLike(decoder)
		if err != nil {
			return nil, err
		}
		if wasNull {
			return nil, nil
		}
		aBoolPtr := &aBool
		return &aBoolPtr, nil
	}

}

func decodeBoolLike(decoder *gojay.Decoder) (bool, bool, error) {
	var value interface{}
	if err := decoder.Interface(&value); err != nil {
		return false, false, err
	}

	switch actual := value.(type) {
	case nil:
		return false, true, nil
	case bool:
		return actual, false, nil
	case float64:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case float32:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case int:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case int8:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case int16:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case int32:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case int64:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case uint:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case uint8:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case uint16:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case uint32:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	case uint64:
		switch actual {
		case 0:
			return false, false, nil
		case 1:
			return true, false, nil
		}
	}

	return false, false, fmt.Errorf("Cannot unmarshal JSON to type '*bool'")
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
			aFloat, _, err := decodeFloat64Value(decoder)
			if err != nil {
				return nil, err
			}
			return &aFloat, nil
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		aFloat, wasNull, err := decodeFloat64Value(decoder)
		if err != nil {
			return nil, err
		}
		if wasNull {
			return nil, nil
		}
		floatPtr := &aFloat
		return &floatPtr, nil
	}
}

func float32Decoder(ptr bool) DecoderFn {
	if !ptr {
		return func(decoder *gojay.Decoder) (interface{}, error) {
			aFloat, _, err := decodeFloat32Value(decoder)
			if err != nil {
				return nil, err
			}
			return &aFloat, nil
		}
	}

	return func(decoder *gojay.Decoder) (interface{}, error) {
		aFloat, wasNull, err := decodeFloat32Value(decoder)
		if err != nil {
			return nil, err
		}
		if wasNull {
			return nil, nil
		}
		float32Ptr := &aFloat
		return &float32Ptr, nil
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
