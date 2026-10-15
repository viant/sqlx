package cache

import "testing"

func TestTemporalCacheCompatibilityIsDirectional(t *testing.T) {
	for _, test := range []struct {
		destination string
		cached      string
		want        bool
	}{
		{"string", "time.Time", true},
		{"time.Time", "string", false},
		{"int", "time.Time", false},
		{"string", "int", false},
		{"string", "[]uint8", false},
	} {
		t.Run(test.destination+"_from_"+test.cached, func(t *testing.T) {
			if got := isCompatibleCacheType(test.destination, test.cached); got != test.want {
				t.Fatalf("compatibility = %v, want %v", got, test.want)
			}
		})
	}
}

func TestProjectedTimestampSupportsTextScanDestination(t *testing.T) {
	field := &Field{ColumnName: "PRIMARY_START_DATE", ColumnScanType: "time.Time"}
	if err := field.Init(); err != nil {
		t.Fatal(err)
	}
	entry := &Entry{Meta: Meta{Fields: []*Field{field}, ProjectedIndexes: []int{0}}}
	var text *string
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&text})
	if !holder.Match(entry) {
		t.Fatalf("timestamp metadata must support the textual temporal scanner: %+v", holder.Mismatch(entry))
	}
	entry.ScanTypes = holder
	entry.Data = []byte(`["2026-10-12T14:00:00.123456-07:00"]`)
	if err := NewProjectedScanner(entry, []int{0}, holder, nil)(&text); err != nil {
		t.Fatal(err)
	}
	if text == nil || *text != "2026-10-12T14:00:00.123456-07:00" {
		t.Fatalf("temporal text changed: %v", text)
	}
	entry.Data = []byte(`[null]`)
	if err := NewProjectedScanner(entry, []int{0}, holder, nil)(&text); err != nil {
		t.Fatal(err)
	}
	if text != nil {
		t.Fatalf("NULL temporal field was not retained: %v", text)
	}
}

func TestScanTypeHolderMatch_PreservesStoredTypeOrderForProjectedEntry(t *testing.T) {
	fields := []*Field{
		{ColumnName: "campaign_id", ColumnScanType: "int"},
		{ColumnName: "day", ColumnScanType: "string"},
		{ColumnName: "spend", ColumnScanType: "float64"},
		{ColumnName: "clicks", ColumnScanType: "int"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}

	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			Type:             []string{"int", "string", "float64", "int"},
			ProjectedIndexes: []int{0, 3, 2},
		},
	}

	var campaignID int
	var clicks int
	var spend float64
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&campaignID, &clicks, &spend})

	if !holder.Match(entry) {
		t.Fatalf("expected first projected type match to succeed")
	}
	if !holder.Match(entry) {
		t.Fatalf("expected repeated projected type match to succeed")
	}

	if got, want := entry.Meta.Type, []string{"int", "string", "float64", "int"}; len(got) != len(want) {
		t.Fatalf("unexpected stored types length %v, want %v", len(got), len(want))
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("stored-order types mutated to %v, want %v", got, want)
			}
		}
	}

	if got, want := entry.Meta.EffectiveType(), []string{"int", "int", "float64"}; len(got) != len(want) {
		t.Fatalf("unexpected effective types length %v, want %v", len(got), len(want))
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("unexpected effective types %v, want %v", got, want)
			}
		}
	}
}
