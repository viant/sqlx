package cache

import "testing"

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
