package cache

import "testing"

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
