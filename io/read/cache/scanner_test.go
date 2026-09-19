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

func TestScannerEscapedStringNullReuse(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			fields := []*Field{
				{ColumnName: "headline", ColumnScanType: "string"},
				{ColumnName: "count_value", ColumnScanType: "int"},
				{ColumnName: "note", ColumnScanType: "string"},
			}
			for _, field := range fields {
				if err := field.Init(); err != nil {
					t.Fatalf("field init error = %v", err)
				}
			}
			entry := &Entry{
				Meta: Meta{
					Fields:           fields,
					ProjectedIndexes: []int{0, 1, 2},
				},
			}
			var headline string
			var count *int
			var note *string
			dest := []any{&headline, &count, &note}
			holder := &ScanTypeHolder{}
			holder.InitType(dest)
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, holder, nil)
			}
			payloads := []struct {
				data         string
				wantHeadline string
				wantCount    *int
				wantNote     *string
			}{
				{
					data:         `["FoxNews:US\u0026WorldHeadlines",null,null]`,
					wantHeadline: "FoxNews:US&WorldHeadlines",
				},
				{
					data:         `["Plain",7,"ready"]`,
					wantHeadline: "Plain",
					wantCount:    intPtr(7),
					wantNote:     stringPtr("ready"),
				},
				{
					data:         `["Escaped\\Path",null,null]`,
					wantHeadline: `Escaped\Path`,
				},
				{
					data:         `["Quoted\"Headline",null,null]`,
					wantHeadline: `Quoted"Headline`,
				},
			}
			for _, payload := range payloads {
				entry.Data = []byte(payload.data)
				if err := scan(dest...); err != nil {
					t.Fatal(err)
				}
				if headline != payload.wantHeadline {
					t.Fatalf("headline got %q want %q", headline, payload.wantHeadline)
				}
				if !reflect.DeepEqual(count, payload.wantCount) {
					t.Fatalf("count got %#v want %#v", count, payload.wantCount)
				}
				if !reflect.DeepEqual(note, payload.wantNote) {
					t.Fatalf("note got %#v want %#v", note, payload.wantNote)
				}
			}
		})
	}
}

func TestScannerScalarNullReuse(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			fields := []*Field{{ColumnName: "count_value", ColumnScanType: "int"}}
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
			var count int
			dest := []any{&count}
			holder := &ScanTypeHolder{}
			holder.InitType(dest)
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, holder, nil)
			}

			entry.Data = []byte(`[7]`)
			if err := scan(dest...); err != nil {
				t.Fatal(err)
			}
			if count != 7 {
				t.Fatalf("count got %d want 7", count)
			}

			entry.Data = []byte(`[null]`)
			err := scan(dest...)
			if err == nil || err.Error() != "converting NULL to int is unsupported" {
				t.Fatalf("err = %v, want converting NULL to int is unsupported", err)
			}
			if count != 7 {
				t.Fatalf("count got %d want preserved stale value on error", count)
			}
		})
	}
}

func TestScannerNullableNonPointerShapesAcceptNull(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			fields := []*Field{
				{ColumnName: "any_value", ColumnScanType: "interface {}"},
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
			var anyValue interface{}
			dest := []any{&anyValue}
			holder := &ScanTypeHolder{}
			holder.InitType(dest)
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, holder, nil)
			}

			entry.Data = []byte(`[7]`)
			if err := scan(dest...); err != nil {
				t.Fatal(err)
			}
			if actual, ok := anyValue.(float64); !ok || actual != 7 {
				t.Fatalf("anyValue got %#v want float64(7)", anyValue)
			}

			entry.Data = []byte(`[null]`)
			if err := scan(dest...); err != nil {
				t.Fatal(err)
			}
			if anyValue != nil {
				t.Fatalf("anyValue got %#v want nil", anyValue)
			}
		})
	}
}

func TestScannerByteSliceNullReuse(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			fields := []*Field{
				{ColumnName: "bytes_value", ColumnScanType: "string"},
				{ColumnName: "raw_value", ColumnScanType: "string"},
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
			var bytesValue []byte
			var rawValue sql.RawBytes
			dest := []any{&bytesValue, &rawValue}
			holder := &ScanTypeHolder{}
			holder.InitType(dest)
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, holder, nil)
			}

			entry.Data = []byte(`["AQI=","AwQ="]`)
			if err := scan(dest...); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(bytesValue, []byte{1, 2}) {
				t.Fatalf("bytesValue got %#v want %#v", bytesValue, []byte{1, 2})
			}
			if !reflect.DeepEqual([]byte(rawValue), []byte{3, 4}) {
				t.Fatalf("rawValue got %#v want %#v", []byte(rawValue), []byte{3, 4})
			}

			entry.Data = []byte(`[null,null]`)
			if err := scan(dest...); err != nil {
				t.Fatal(err)
			}
			if bytesValue != nil {
				t.Fatalf("bytesValue got %#v want nil", bytesValue)
			}
			if rawValue != nil {
				t.Fatalf("rawValue got %#v want nil", []byte(rawValue))
			}
		})
	}
}

func TestScannerTypedSliceReuse(t *testing.T) {
	for _, projected := range []bool{false, true} {
		name := "full"
		if projected {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			fields := []*Field{{ColumnName: "campaign_ids", ColumnScanType: "[]int"}}
			projectedIndexes := []int{0}
			payloads := []string{`[[100,200]]`, `[[]]`, `[null]`}
			if projected {
				fields = []*Field{
					{ColumnName: "campaign_ids", ColumnScanType: "[]int"},
					{ColumnName: "label", ColumnScanType: "string"},
				}
				projectedIndexes = []int{0}
				payloads = []string{`[[100,200],"ignored"]`, `[[],"ignored"]`, `[null,"ignored"]`}
			}
			for _, field := range fields {
				if err := field.Init(); err != nil {
					t.Fatalf("field init error = %v", err)
				}
			}
			entry := &Entry{
				Meta: Meta{
					Fields:           fields,
					ProjectedIndexes: projectedIndexes,
				},
			}
			var campaignIDs []int
			holder := &ScanTypeHolder{}
			holder.InitType([]interface{}{&campaignIDs})
			scan := NewScanner(holder, nil).New(entry)
			if projected {
				scan = NewProjectedScanner(entry, entry.Meta.ProjectedIndexes, holder, nil)
			}

			entry.Data = []byte(payloads[0])
			if err := scan(&campaignIDs); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(campaignIDs, []int{100, 200}) {
				t.Fatalf("campaignIDs got %#v want %#v", campaignIDs, []int{100, 200})
			}

			entry.Data = []byte(payloads[1])
			if err := scan(&campaignIDs); err != nil {
				t.Fatal(err)
			}
			if campaignIDs == nil || len(campaignIDs) != 0 {
				t.Fatalf("campaignIDs got %#v want empty slice", campaignIDs)
			}

			entry.Data = []byte(payloads[2])
			if err := scan(&campaignIDs); err != nil {
				t.Fatal(err)
			}
			if campaignIDs != nil {
				t.Fatalf("campaignIDs got %#v want nil", campaignIDs)
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
