package sink

import "testing"

func TestColumnKnownNotNull(t *testing.T) {
	for _, value := range []string{"NO", "no", "N", "false", "FALSE", "f", "0", " 0 "} {
		if !(&Column{Nullable: value}).IsNotNull() {
			t.Errorf("known NOT NULL %q lost", value)
		}
	}
	for _, value := range []string{"YES", "Y", "true", "T", "1", "", " ", "unknown", "?", "null"} {
		if (&Column{Nullable: value}).IsNotNull() {
			t.Errorf("invented NOT NULL from %q", value)
		}
	}
	var missing *Column
	if missing.IsNotNull() {
		t.Fatal("missing column treated as NOT NULL")
	}
}
