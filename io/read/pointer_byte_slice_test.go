package read

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPointerByteSliceScanner_Scan(t *testing.T) {
	bytes := []byte{1, 2, 3}
	var actual *[]byte
	scanner := newPointerByteSliceScanner(&actual)

	if err := scanner.Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bytes, *actual) {
		t.Fatalf("bytes = %v, want %v", *actual, bytes)
	}
	if &bytes[0] == &(*actual)[0] {
		t.Fatal("expected scanned bytes to be copied")
	}

	encoded, err := json.Marshal(scanner)
	if err != nil {
		t.Fatal(err)
	}
	if actual := string(encoded); actual != `"AQID"` {
		t.Fatalf("encoded = %s, want %q", actual, `"AQID"`)
	}

	if err := scanner.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if actual != nil {
		t.Fatalf("actual = %v, want nil", *actual)
	}
}

func TestPointerByteSliceScanner_RejectsNonByteSlice(t *testing.T) {
	var actual *[]byte
	err := newPointerByteSliceScanner(&actual).Scan("not bytes")
	if err == nil {
		t.Fatal("expected scan to reject non-byte-slice source")
	}
}

func TestPointerByteSliceScanner_ScanGenericByteSlice(t *testing.T) {
	bytes := []byte{4, 5, 6}
	var actual []byte
	scanner := newPointerByteSliceScanner(&actual)

	if err := scanner.Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bytes, actual) {
		t.Fatalf("bytes = %v, want %v", actual, bytes)
	}
}
