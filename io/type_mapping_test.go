package io

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/xreflect"
)

func TestParseType(t *testing.T) {
	testCases := []struct {
		name     string
		dbType   string
		expected reflect.Type
	}{
		{name: "UUID", dbType: "uuid", expected: reflect.TypeOf("")},
		{name: "GUID", dbType: "guid", expected: reflect.TypeOf("")},
		{name: "timestamp with timezone", dbType: "timestamptz", expected: reflect.TypeOf(time.Time{})},
		{name: "real", dbType: "real", expected: reflect.TypeOf(float64(0))},
		{name: "JSON remains bytes in stage 5A", dbType: "json", expected: reflect.TypeOf([]byte{})},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, dbType := range []string{testCase.dbType, strings.ToUpper(testCase.dbType)} {
				actual, ok := ParseType(dbType)
				if !ok {
					t.Fatalf("expected %q to be recognized", dbType)
				}
				if actual != testCase.expected {
					t.Fatalf("expected %q to map to %v, got %v", dbType, testCase.expected, actual)
				}
			}
		})
	}

	if actual, ok := ParseType("unknown_type"); ok || actual != nil {
		t.Fatalf("expected unknown type to remain unrecognized, got %v, %v", actual, ok)
	}
}

func TestNormalizeColumnType(t *testing.T) {
	testCases := []struct {
		name     string
		scanType reflect.Type
		dbType   string
		expected reflect.Type
	}{
		{name: "nil scan metadata", scanType: nil, dbType: "unknown_type", expected: xreflect.InterfaceType},
		{name: "UUID overrides interface scan type", scanType: xreflect.InterfaceType, dbType: "UUID", expected: reflect.TypeOf("")},
		{name: "REAL uses float64", scanType: reflect.TypeOf(float32(0)), dbType: "REAL", expected: reflect.TypeOf(float64(0))},
		{name: "TIMESTAMPTZ uses time", scanType: reflect.TypeOf(""), dbType: "TIMESTAMPTZ", expected: reflect.TypeOf(time.Time{})},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := NormalizeColumnType(testCase.scanType, testCase.dbType)
			if actual != testCase.expected {
				t.Fatalf("expected %q to normalize to %v, got %v", testCase.dbType, testCase.expected, actual)
			}
		})
	}
}

func TestEnsureScanType(t *testing.T) {
	interfaceScanType := reflect.TypeOf((*interface{})(nil)).Elem()
	testCases := []struct {
		name     string
		dbType   string
		scanType reflect.Type
		expected reflect.Type
	}{
		{name: "nil UUID", dbType: "UUID", scanType: nil, expected: typeString},
		{name: "interface UUID", dbType: "uuid", scanType: interfaceScanType, expected: typeString},
		{name: "UUID array name", dbType: "_uuid", scanType: nil, expected: typeString},
		{name: "concrete scan type is preserved", dbType: "uuid", scanType: typeBytes, expected: typeBytes},
		{name: "unknown fallback", dbType: "custom", scanType: nil, expected: interfaceType},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := ensureScanType(testCase.dbType, testCase.scanType)
			if actual != testCase.expected {
				t.Fatalf("expected %q with scan type %v to use %v, got %v", testCase.dbType, testCase.scanType, testCase.expected, actual)
			}
		})
	}
}
