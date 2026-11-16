package metadata

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

func TestFetchToString_PropagatesRowError(t *testing.T) {
	for _, wantError := range []error{errors.New("first row transport failure"), context.DeadlineExceeded, context.Canceled} {
		t.Run(wantError.Error(), func(t *testing.T) {
			db, _ := openVersionDetectionDB(t, versionDetectionResponse{rowError: wantError})
			rows, err := db.QueryContext(context.Background(), "SELECT version")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var value string
			if err := fetchToString(rows, &value); !errors.Is(err, wantError) {
				t.Fatalf("fetchToString() error = %v, want %v", err, wantError)
			}
		})
	}
}

func TestFetchToString_ZeroRowsPreservesValue(t *testing.T) {
	db, _ := openVersionDetectionDB(t, versionDetectionResponse{})
	rows, err := db.QueryContext(context.Background(), "SELECT version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	value := "unchanged"
	if err := fetchToString(rows, &value); err != nil {
		t.Fatalf("fetchToString() error = %v, want nil for zero rows", err)
	}
	if value != "unchanged" {
		t.Fatalf("fetchToString() value = %q, want unchanged", value)
	}
}

func TestFetchToStrings_PropagatesRowError(t *testing.T) {
	transportErr := errors.New("row transport failure")
	for _, testCase := range []struct {
		name      string
		rows      [][]driver.Value
		wantError error
	}{
		{name: "first_row", wantError: transportErr},
		{name: "partial_result", rows: [][]driver.Value{{"first"}, {"second"}}, wantError: transportErr},
		{name: "deadline", wantError: context.DeadlineExceeded},
		{name: "canceled", wantError: context.Canceled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			db, _ := openVersionDetectionDB(t, versionDetectionResponse{rows: testCase.rows, rowError: testCase.wantError})
			rows, err := db.QueryContext(context.Background(), "SELECT version")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var values []string
			if err := fetchToStrings(rows, &values); !errors.Is(err, testCase.wantError) {
				t.Fatalf("fetchToStrings() error = %v, want %v", err, testCase.wantError)
			}
			var wantValues []string
			for _, row := range testCase.rows {
				wantValues = append(wantValues, row[0].(string))
			}
			if !reflect.DeepEqual(values, wantValues) {
				t.Fatalf("fetchToStrings() values = %v, want partial result %v", values, wantValues)
			}
		})
	}
}
