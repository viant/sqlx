package cache

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestScannerStandardNullTypesReuse(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			var i sql.NullInt64
			var i32 sql.NullInt32
			var i16 sql.NullInt16
			var b sql.NullByte
			var s sql.NullString
			var flag sql.NullBool
			var f sql.NullFloat64
			var tm sql.NullTime
			dest := []any{&i, &i32, &i16, &b, &s, &flag, &f, &tm}
			holder := &ScanTypeHolder{}
			holder.InitType(dest)
			entry := &Entry{}
			indexes := make([]int, len(dest))
			for j, scanType := range []string{"int64", "int64", "int64", "int64", "string", "bool", "float64", "time.Time"} {
				indexes[j] = j
				field := &Field{ColumnName: "value", ColumnScanType: scanType}
				if err := field.Init(); err != nil {
					t.Fatal(err)
				}
				entry.Meta.Fields = append(entry.Meta.Fields, field)
			}
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, indexes, holder, nil)
			}
			for _, valid := range []bool{true, false, true, false} {
				want := []any{sql.NullInt64{Int64: 0, Valid: valid}, sql.NullInt32{Int32: 0, Valid: valid}, sql.NullInt16{Int16: 0, Valid: valid}, sql.NullByte{Byte: 0, Valid: valid}, sql.NullString{String: "", Valid: valid}, sql.NullBool{Bool: false, Valid: valid}, sql.NullFloat64{Float64: 0, Valid: valid}, sql.NullTime{Valid: valid}}
				if valid {
					want[7] = sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
				}
				if valid {
					var err error
					entry.Data, err = json.Marshal(want)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					entry.Data = []byte(`[null,null,null,null,null,null,null,null]`)
				}
				if err := scan(dest...); err != nil {
					t.Fatal(err)
				}
				for j, value := range dest {
					if !reflect.DeepEqual(reflect.ValueOf(value).Elem().Interface(), want[j]) {
						t.Fatalf("destination %d got %#v want %#v", j, value, want[j])
					}
				}
			}
		})
	}
}

func TestNewProjectedScanner_ScansRequestedSubsetInRequestedOrder(t *testing.T) {
	fields := []*Field{
		{ColumnName: "order_id", ColumnScanType: "int"},
		{ColumnName: "bids", ColumnScanType: "int"},
		{ColumnName: "impressions", ColumnScanType: "int"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{2, 0},
		},
		Data: []byte(`[101,20,300]`),
	}

	scanner := NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, nil, nil)
	var impressions int
	var orderID int
	if err := scanner(&impressions, &orderID); err != nil {
		t.Fatalf("scanner error = %v", err)
	}

	if impressions != 300 {
		t.Fatalf("unexpected impressions %d", impressions)
	}
	if orderID != 101 {
		t.Fatalf("unexpected order ID %d", orderID)
	}
}

func TestProjectedEntryTypeMatch_UsesStoredFieldTypesBeforeFirstProjectionRead(t *testing.T) {
	fields := []*Field{
		{ColumnName: "order_id", ColumnScanType: "int"},
		{ColumnName: "bids", ColumnScanType: "int"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{1},
		},
	}

	intHolder := &ScanTypeHolder{}
	intHolder.InitType([]interface{}{new(int)})
	if !intHolder.Match(entry) {
		t.Fatalf("expected projected entry to match stored int field type")
	}

	stringEntry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{1},
		},
	}
	stringHolder := &ScanTypeHolder{}
	stringHolder.InitType([]interface{}{new(string)})
	if stringHolder.Match(stringEntry) {
		t.Fatalf("expected projected entry type check to reject incompatible destination type")
	}
}

func TestProjectedEntryTypeMatch_NormalizesNullablePointerDestinations(t *testing.T) {
	fields := []*Field{
		{ColumnName: "audience_id", ColumnScanType: "int"},
		{ColumnName: "spend", ColumnScanType: "float64"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0, 1},
		},
	}

	var audienceID *int
	var spend *float64
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&audienceID, &spend})
	if got, want := holder.dataTypes, []string{"int", "float64"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected normalized compat types %v, want %v", got, want)
	}
	if !holder.Match(entry) {
		t.Fatalf("expected nullable pointer destinations to match projected numeric field types")
	}
}

func TestProjectedEntryTypeMatch_AllowsBoolDestinationForStoredInt(t *testing.T) {
	fields := []*Field{
		{ColumnName: "has_ai_media_plan", ColumnScanType: "int"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0},
		},
	}

	var hasAI *bool
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&hasAI})
	if got, want := holder.dataTypes, []string{"bool"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected normalized compat types %v, want %v", got, want)
	}
	if !holder.Match(entry) {
		t.Fatalf("expected bool destination to match stored int cache type")
	}
}

func TestProjectedEntryTypeMatch_AllowsFloat64DestinationForStoredInt(t *testing.T) {
	fields := []*Field{
		{ColumnName: "media_plan_total_budget", ColumnScanType: "int"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0},
		},
	}

	var budget *float64
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&budget})
	if got, want := holder.dataTypes, []string{"float64"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected normalized compat types %v, want %v", got, want)
	}
	if !holder.Match(entry) {
		t.Fatalf("expected float64 destination to match stored int cache type")
	}
}

func TestProjectedEntryTypeMatch_RejectsFloat64DestinationForStoredUint64(t *testing.T) {
	fields := []*Field{
		{ColumnName: "media_plan_total_budget", ColumnScanType: "uint64"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0},
		},
	}

	var budget *float64
	holder := &ScanTypeHolder{}
	holder.InitType([]interface{}{&budget})
	if holder.Match(entry) {
		t.Fatalf("expected float64 destination to reject stored uint64 cache type")
	}
	mismatch := holder.Mismatch(entry)
	if mismatch == nil {
		t.Fatalf("expected mismatch details")
	}
	if got, want := mismatch.Index, 0; got != want {
		t.Fatalf("unexpected mismatch index %v, want %v", got, want)
	}
	if got, want := mismatch.NormalizedCachedType, "uint64"; got != want {
		t.Fatalf("unexpected normalized cached type %q, want %q", got, want)
	}
}

func TestNewProjectedScanner_UsesRuntimeNullableDestinationShapes(t *testing.T) {
	fields := []*Field{
		{ColumnName: "audience_id", ColumnScanType: "int"},
		{ColumnName: "spend", ColumnScanType: "float64"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0, 1},
		},
		Data: []byte(`[101,2.5]`),
	}

	var audienceID *int
	var spend *float64
	typeHolder := &ScanTypeHolder{}
	typeHolder.InitType([]interface{}{&audienceID, &spend})

	scanner := NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, typeHolder, nil)
	if err := scanner(&audienceID, &spend); err != nil {
		t.Fatalf("scanner error = %v", err)
	}

	if audienceID == nil || *audienceID != 101 {
		t.Fatalf("unexpected audienceID %v", audienceID)
	}
	if spend == nil || *spend != 2.5 {
		t.Fatalf("unexpected spend %v", spend)
	}
}

func TestNewProjectedScanner_ResetsNullableDestinationsOnNull(t *testing.T) {
	fields := []*Field{
		{ColumnName: "audience_id", ColumnScanType: "int"},
		{ColumnName: "spend", ColumnScanType: "float64"},
	}
	for _, field := range fields {
		if err := field.Init(); err != nil {
			t.Fatalf("field init error = %v", err)
		}
	}
	entry := &Entry{
		Meta: Meta{
			Fields:           fields,
			ProjectedIndexes: []int{0, 1},
		},
		Data: []byte(`[101,2.5]`),
	}

	var audienceID *int
	var spend *float64
	typeHolder := &ScanTypeHolder{}
	typeHolder.InitType([]interface{}{&audienceID, &spend})

	scanner := NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, typeHolder, nil)
	if err := scanner(&audienceID, &spend); err != nil {
		t.Fatalf("scanner error = %v", err)
	}
	if audienceID == nil || *audienceID != 101 {
		t.Fatalf("unexpected audienceID %v", audienceID)
	}
	if spend == nil || *spend != 2.5 {
		t.Fatalf("unexpected spend %v", spend)
	}

	entry.Data = []byte(`[null,null]`)
	if err := scanner(&audienceID, &spend); err != nil {
		t.Fatalf("scanner error = %v", err)
	}
	if audienceID != nil {
		t.Fatalf("expected audienceID to reset to nil, got %v", audienceID)
	}
	if spend != nil {
		t.Fatalf("expected spend to reset to nil, got %v", spend)
	}
}
