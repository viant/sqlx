package cache

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"reflect"
)

type Scanner struct {
	typeHolder *ScanTypeHolder
	recorder   Recorder
}

func NewScanner(typeHolder *ScanTypeHolder, recorder Recorder) *Scanner {
	return &Scanner{
		typeHolder: typeHolder,
		recorder:   recorder,
	}
}

func (c *Scanner) New(e *Entry) ScannerFn {
	var decoder *Decoder
	var err error

	return func(values ...interface{}) error {
		if len(values) != len(c.typeHolder.scanTypes) {
			return fmt.Errorf("invalid cache format, expected to have %v values but got %v", len(values), len(c.typeHolder.scanTypes))
		}

		if decoder == nil {
			decoder = NewDecoder(c.typeHolder.scanTypes, e.Data)
		}

		decoder.Data = e.Data
		if err = gojay.UnmarshalJSONArray(e.Data, decoder); err != nil {
			return err
		}

		for i, cachedValue := range decoder.values {
			if cachedValue == nil {
				continue
			}

			destPtr := xunsafe.AsPointer(values[i])
			srcPtr := xunsafe.AsPointer(cachedValue)
			if destPtr == nil || srcPtr == nil {
				continue
			}

			xunsafe.Copy(destPtr, srcPtr, int(c.typeHolder.scanTypes[i].Size()))
		}

		e.index++
		decoder.reset()

		if c.recorder != nil {
			c.recorder.ScanValues(values)
		}

		return err
	}
}

func NewProjectedScanner(entry *Entry, indexes []int, typeHolder *ScanTypeHolder, recorder Recorder) ScannerFn {
	scanTypes := projectedScanTypes(entry, indexes, typeHolder)
	var decoder *Decoder
	var err error

	return func(values ...interface{}) error {
		if len(values) != len(indexes) {
			return fmt.Errorf("invalid projected cache format, expected to have %v values but got %v", len(indexes), len(values))
		}

		if decoder == nil {
			decoder = NewDecoder(scanTypes, entry.Data)
		}

		decoder.Data = entry.Data
		if err = gojay.UnmarshalJSONArray(entry.Data, decoder); err != nil {
			return err
		}

		for destIndex, storedIndex := range indexes {
			if storedIndex < 0 || storedIndex >= len(decoder.values) {
				return fmt.Errorf("invalid projected cache format, stored index %v out of range %v", storedIndex, len(decoder.values))
			}
			cachedValue := decoder.values[storedIndex]
			if cachedValue == nil {
				zeroScanDestination(values[destIndex])
				continue
			}

			destPtr := xunsafe.AsPointer(values[destIndex])
			srcPtr := xunsafe.AsPointer(cachedValue)
			if destPtr == nil || srcPtr == nil {
				continue
			}

			xunsafe.Copy(destPtr, srcPtr, int(scanTypes[storedIndex].Size()))
		}

		entry.index++
		decoder.reset()

		if recorder != nil {
			recorder.ScanValues(values)
		}

		return err
	}
}

func projectedScanTypes(entry *Entry, indexes []int, typeHolder *ScanTypeHolder) []reflect.Type {
	scanTypes := make([]reflect.Type, len(entry.Meta.Fields))
	for i, field := range entry.Meta.Fields {
		scanTypes[i] = field.ScanType()
	}
	if typeHolder == nil {
		return scanTypes
	}
	for destIndex, storedIndex := range indexes {
		if destIndex >= len(typeHolder.scanTypes) || storedIndex < 0 || storedIndex >= len(scanTypes) {
			continue
		}
		if typeHolder.scanTypes[destIndex] == nil {
			continue
		}
		scanTypes[storedIndex] = typeHolder.scanTypes[destIndex]
	}
	return scanTypes
}

func zeroScanDestination(value interface{}) {
	if value == nil {
		return
	}
	rValue := reflect.ValueOf(value)
	if rValue.Kind() != reflect.Ptr || rValue.IsNil() {
		return
	}
	rValue.Elem().Set(reflect.Zero(rValue.Elem().Type()))
}
