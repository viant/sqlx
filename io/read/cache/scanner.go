package cache

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
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
			decoder = NewDecoder(c.typeHolder.scanTypes)
		}

		if err = gojay.UnmarshalJSONArray(e.Data, decoder); err != nil {
			return err
		}

		for i, cachedValue := range decoder.values {
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
