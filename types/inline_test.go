package types

import (
	"reflect"
	"testing"
	"time"
)

func Test_InlinedTypes(t *testing.T) {

	// Define the expected inlined type
	expectedFields := []struct {
		Name string
		Type reflect.Kind
	}{
		{"Xid", reflect.Ptr},
		{"Timestamp", reflect.Ptr},
		{"Url", reflect.Ptr},
		{"IpInfo", reflect.Struct},
		{"UserAgent", reflect.Struct},
		{"Parameters", reflect.Slice},
	}

	// Get the type of the root struct
	requestType := reflect.TypeOf(Request{})
	// Generate the inlined type
	inlinedType := InlineStruct(requestType)

	// Check that the inlined type is a struct
	if inlinedType.Kind() != reflect.Struct {
		t.Errorf("Expected inlined type to be a struct, got %s", inlinedType.Kind())
	}

	// Check the number of fields
	if inlinedType.NumField() != len(expectedFields) {
		t.Errorf("Expected %d fields, got %d", len(expectedFields), inlinedType.NumField())
	}

	// Check each field
	for i, expectedField := range expectedFields {
		field := inlinedType.Field(i)
		if field.Name != expectedField.Name {
			t.Errorf("Expected field name %s, got %s", expectedField.Name, field.Name)
		}
		if field.Type.Kind() != expectedField.Type {
			t.Errorf("Expected field %s to be of kind %s, got %s", field.Name, expectedField.Type, field.Type.Kind())
		}
	}

	// Check that IpInfo and UserAgent fields are inlined structs
	ipInfoField, _ := inlinedType.FieldByName("IpInfo")
	if ipInfoField.Type.Kind() != reflect.Struct {
		t.Errorf("Expected IpInfo to be a struct, got %s", ipInfoField.Type.Kind())
	}
	if ipInfoField.Type.NumField() == 0 {
		t.Errorf("Expected IpInfo to have fields, got 0")
	}

	userAgentField, _ := inlinedType.FieldByName("UserAgent")
	if userAgentField.Type.Kind() != reflect.Struct {
		t.Errorf("Expected UserAgent to be a struct, got %s", userAgentField.Type.Kind())
	}
	if userAgentField.Type.NumField() == 0 {
		t.Errorf("Expected UserAgent to have fields, got 0")
	}

	// Create a new instance of the inlined type
	newInstance := reflect.New(inlinedType).Elem()

	// Set some values to test
	xidValue := "TestXID"
	newInstance.FieldByName("Xid").Set(reflect.ValueOf(&xidValue))

	if *newInstance.FieldByName("Xid").Interface().(*string) != xidValue {
		t.Errorf("Expected Xid to be %s, got %s", xidValue, *newInstance.FieldByName("Xid").Interface().(*string))
	}
}

// Example types defined in the root package
type IpInfo struct {
	Ip       *string `json:"ip,omitempty"`
	CityCode *int    `json:"cityCode,omitempty"`
	City     *string `json:"city,omitempty"`
	State    *string `json:"state,omitempty"`
}

type UserAgent struct {
	UserAgent           *string `json:"userAgent,omitempty"`
	Browser             *string `json:"browser,omitempty"`
	DeviceName          *string `json:"deviceName,omitempty"`
	HardwareName        *string `json:"hardwareName,omitempty"`
	IsJavascriptCapable *bool   `json:"isJavascriptCapable,omitempty"`
	StructuredUserAgent struct {
		Browsers *string `json:"browsers,omitempty"`
		Platform *string `json:"platform,omitempty"`
		Model    *string `json:"model,omitempty"`
		Arch     *string `json:"arch,omitempty"`
		Bitness  *string `json:"bitness,omitempty"`
		IsMobile *bool   `json:"isMobile,omitempty"`
		Source   *string `json:"source,omitempty"`
	} `json:"structuredUserAgent,omitempty"`
}

// Parameter struct
type Parameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Root struct
type Request struct {
	Xid        *string      `json:"xid,omitempty"`
	Timestamp  *time.Time   `json:"timestamp,omitempty"`
	Url        *string      `json:"url,omitempty"`
	IpInfo     IpInfo       `json:"ipInfo,omitempty"`
	UserAgent  UserAgent    `json:"userAgent,omitempty"`
	Parameters []*Parameter `json:"parameters,omitempty"`
}
