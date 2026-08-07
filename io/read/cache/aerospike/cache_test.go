package aerospike

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/read/cache"
	"reflect"
	"testing"
)

func TestWarmupDebugEnabled(t *testing.T) {
	testCases := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", value: "", want: false},
		{name: "true", value: "true", want: true},
		{name: "one", value: "1", want: true},
		{name: "mixed case", value: " On ", want: true},
		{name: "false", value: "false", want: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("SQLX_AEROSPIKE_WARMUP_DEBUG", testCase.value)
			if got := warmupDebugEnabled(); got != testCase.want {
				t.Fatalf("warmupDebugEnabled() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestCacheResolveIndexIdentity_DefaultsToExecutionSQLAndArgs(t *testing.T) {
	aCache := &Cache{}

	identitySQL, identityArgs, identityArgsMarshal, meta, err := aCache.resolveIndexIdentity(
		"SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		[]interface{}{"tenant-a", 2002},
	)
	if err != nil {
		t.Fatalf("resolveIndexIdentity() error = %v", err)
	}

	if identitySQL != "SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?" {
		t.Fatalf("unexpected identity SQL %q", identitySQL)
	}
	if len(identityArgs) != 2 {
		t.Fatalf("expected execution args to be used by default, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != `["tenant-a",2002]` {
		t.Fatalf("unexpected marshaled identity args %s", string(identityArgsMarshal))
	}
	if meta.Source != "execution" {
		t.Fatalf("expected execution identity source, got %q", meta.Source)
	}
}

func TestCacheResolveIndexIdentity_UsesMatcherWarmupIdentity(t *testing.T) {
	aCache := &Cache{}
	matcher := &cache.ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		Args:         []interface{}{"tenant-a", 2002},
		IdentitySQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ?",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, meta, err := aCache.resolveIndexIdentity(
		matcher.SQL,
		matcher.Args,
		matcher,
	)
	if err != nil {
		t.Fatalf("resolveIndexIdentity() error = %v", err)
	}

	if identitySQL != matcher.IdentitySQL {
		t.Fatalf("expected identity SQL %q, got %q", matcher.IdentitySQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "tenant-a" {
		t.Fatalf("expected matcher identity args [tenant-a], got %v", identityArgs)
	}
	if string(identityArgsMarshal) != `["tenant-a"]` {
		t.Fatalf("unexpected marshaled matcher identity args %s", string(identityArgsMarshal))
	}
	if meta.Source != "explicit" {
		t.Fatalf("expected explicit identity source, got %q", meta.Source)
	}
}

func TestCacheResolveIndexIdentity_RejectsInvalidMatcherIdentity(t *testing.T) {
	aCache := &Cache{}
	matcher := &cache.ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	_, _, _, _, err := aCache.resolveIndexIdentity(matcher.SQL, matcher.Args, matcher)
	if err == nil {
		t.Fatalf("expected resolveIndexIdentity() to reject invalid matcher identity")
	}
}

func TestCanonicalWarmupSQL_NormalizesEquivalentSQL(t *testing.T) {
	writeSQL := "SELECT  t.CAMPAIGN_ID,  t.ID FROM (SELECT\n        cf.CAMPAIGN_ID,\n        cf.ID\n    FROM CI_CAMPAIGN_FLIGHT cf   ) AS t "
	readSQL := "SELECT t.CAMPAIGN_ID, t.ID FROM  (SELECT cf.CAMPAIGN_ID, cf.ID FROM CI_CAMPAIGN_FLIGHT cf)  t"

	canonicalWrite, ok, _ := canonicalWarmupSQL(writeSQL)
	if !ok {
		t.Fatalf("expected write SQL to be canonicalizable")
	}
	canonicalRead, ok, _ := canonicalWarmupSQL(readSQL)
	if !ok {
		t.Fatalf("expected read SQL to be canonicalizable")
	}

	if canonicalWrite != canonicalRead {
		t.Fatalf("expected canonical SQL to match, got write=%q read=%q", canonicalWrite, canonicalRead)
	}
}

func TestCanonicalWarmupSQL_NormalizesEquivalentOperators(t *testing.T) {
	writeSQL := "SELECT t.audience_id FROM(SELECT si.audience_id FROM soft_ineligibilities si JOIN UNNEST(si.feature_rejection_estimates) fr ON 1=1 WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) t"
	readSQL := "SELECT t.audience_id FROM(SELECT si.audience_id FROM soft_ineligibilities si JOIN UNNEST(si.feature_rejection_estimates) fr ON 1 = 1 WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) t"

	canonicalWrite, ok, _ := canonicalWarmupSQL(writeSQL)
	if !ok {
		t.Fatalf("expected write SQL to be canonicalizable")
	}
	canonicalRead, ok, _ := canonicalWarmupSQL(readSQL)
	if !ok {
		t.Fatalf("expected read SQL to be canonicalizable")
	}

	if canonicalWrite != canonicalRead {
		t.Fatalf("expected canonical SQL to match, got write=%q read=%q", canonicalWrite, canonicalRead)
	}
}

func TestCanonicalWarmupIdentityURL_MatchesBetweenWriteAndReadForms(t *testing.T) {
	aCache := &Cache{}
	writeSQL := "SELECT  t.CAMPAIGN_ID,  t.ID FROM (SELECT\n        cf.CAMPAIGN_ID,\n        cf.ID\n    FROM CI_CAMPAIGN_FLIGHT cf   ) AS t "
	readSQL := "SELECT t.CAMPAIGN_ID, t.ID FROM  (SELECT cf.CAMPAIGN_ID, cf.ID FROM CI_CAMPAIGN_FLIGHT cf)  t"
	argsJSON := []byte("[]")

	writeSQL, writeArgs, _ := canonicalWarmupIdentity(writeSQL, argsJSON)
	readSQL, readArgs, _ := canonicalWarmupIdentity(readSQL, argsJSON)

	writeURL, err := aCache.identityURL(writeSQL, nil, writeArgs)
	if err != nil {
		t.Fatalf("identityURL(write) error = %v", err)
	}
	readURL, err := aCache.identityURL(readSQL, nil, readArgs)
	if err != nil {
		t.Fatalf("identityURL(read) error = %v", err)
	}

	if writeURL != readURL {
		t.Fatalf("expected canonical warmup URLs to match, got write=%s read=%s", writeURL, readURL)
	}
}

func TestTryOrderedSQL_AppendsOrderByWhenMissing(t *testing.T) {
	sql := "SELECT order_id, advertiser_time FROM metrics"

	gotSQL, ordered := tryOrderedSQL(sql, "order_id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	want := "SELECT order_id, advertiser_time FROM metrics ORDER BY order_id"
	if gotSQL != want {
		t.Fatalf("unexpected SQL %q, want %q", gotSQL, want)
	}
}

func TestTryOrderedSQL_RecognizesExistingOrderByColumn(t *testing.T) {
	sql := "SELECT order_id, advertiser_time FROM metrics ORDER BY order_id, advertiser_time"

	gotSQL, ordered := tryOrderedSQL(sql, "order_id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	if gotSQL != sql {
		t.Fatalf("unexpected SQL %q, want original %q", gotSQL, sql)
	}
}

func TestTryOrderedSQL_RecognizesQualifiedExistingOrderByColumn(t *testing.T) {
	sql := "SELECT m.order_id, m.advertiser_time FROM metrics m ORDER BY m.order_id DESC, m.advertiser_time"

	gotSQL, ordered := tryOrderedSQL(sql, "order_id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	if gotSQL != sql {
		t.Fatalf("unexpected SQL %q, want original %q", gotSQL, sql)
	}
}

func TestTryOrderedSQL_DoesNotTreatSubstringMatchAsOrdered(t *testing.T) {
	sql := "SELECT id, advertiser_id FROM metrics ORDER BY advertiser_id"

	gotSQL, ordered := tryOrderedSQL(sql, "id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	want := "SELECT * FROM (SELECT id, advertiser_id FROM metrics ORDER BY advertiser_id) AS _sqlx_warmup ORDER BY id"
	if gotSQL != want {
		t.Fatalf("unexpected SQL %q, want %q", gotSQL, want)
	}
}

func TestTryOrderedSQL_WrapsWhenOrderedByDifferentColumn(t *testing.T) {
	sql := "SELECT order_id, advertiser_time FROM metrics ORDER BY advertiser_time"

	gotSQL, ordered := tryOrderedSQL(sql, "order_id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	want := "SELECT * FROM (SELECT order_id, advertiser_time FROM metrics ORDER BY advertiser_time) AS _sqlx_warmup ORDER BY order_id"
	if gotSQL != want {
		t.Fatalf("unexpected SQL %q, want %q", gotSQL, want)
	}
}

func TestTryOrderedSQL_TrimsTrailingSemicolon(t *testing.T) {
	sql := "SELECT order_id FROM metrics;"

	gotSQL, ordered := tryOrderedSQL(sql, "order_id")
	if !ordered {
		t.Fatalf("expected ordered result")
	}
	want := "SELECT order_id FROM metrics ORDER BY order_id"
	if gotSQL != want {
		t.Fatalf("unexpected SQL %q, want %q", gotSQL, want)
	}
}

func TestCacheIndexBy_WritesStoredFieldsOnMarkerRecord(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE metrics (order_id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err = db.Exec(`INSERT INTO metrics(order_id, name) VALUES (7, 'alpha')`); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	records := map[string]as.BinMap{}
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			records[key.String()] = cloneBinMap(binMap)
			return nil
		},
	}

	matcher := &cache.ParmetrizedQuery{
		StoredFields: []cache.ProjectionField{
			{Name: "order_id", ColumnName: "order_id"},
			{Name: "name", ColumnName: "name"},
		},
	}

	result, err := aCache.IndexByWithResult(
		context.Background(),
		db,
		"order_id",
		"SELECT order_id, name FROM metrics",
		nil,
		matcher,
	)
	if err != nil {
		t.Fatalf("IndexBy() error = %v", err)
	}
	if result == nil {
		t.Fatalf("IndexBy() result was nil")
	}
	if result.GroupsWritten != 1 {
		t.Fatalf("IndexBy() GroupsWritten = %d, want 1", result.GroupsWritten)
	}
	if result.WarmupKey == "" {
		t.Fatalf("IndexBy() WarmupKey was empty")
	}
	if result.MarkerKey == "" {
		t.Fatalf("IndexBy() MarkerKey was empty")
	}

	markerKey, err := aCache.key(result.MarkerKey)
	if err != nil {
		t.Fatalf("key(%q) error = %v", result.MarkerKey, err)
	}
	marker, ok := records[markerKey.String()]
	if !ok {
		t.Fatalf("expected marker record for key %q to be written", result.MarkerKey)
	}

	storedFieldsValue, ok := marker[storedFieldsBin]
	if !ok {
		t.Fatalf("expected %s bin on marker record", storedFieldsBin)
	}
	storedFieldsJSON, ok := storedFieldsValue.(string)
	if !ok {
		t.Fatalf("expected %s bin to be string, got %T", storedFieldsBin, storedFieldsValue)
	}

	var storedFields []cache.ProjectionField
	if err = json.Unmarshal([]byte(storedFieldsJSON), &storedFields); err != nil {
		t.Fatalf("json.Unmarshal(stored fields) error = %v", err)
	}
	if len(storedFields) != 2 {
		t.Fatalf("unexpected stored fields count %d", len(storedFields))
	}
	if storedFields[0].Name != "order_id" {
		t.Fatalf("unexpected first stored field %+v", storedFields[0])
	}
}

func TestCacheIndexBy_UnindexedReturnsWarmupKeyWithoutMarkerKey(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE metrics (name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err = db.Exec(`INSERT INTO metrics(name) VALUES ('alpha')`); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	writes := 0
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			writes++
			return nil
		},
	}

	result, err := aCache.IndexByWithResult(
		context.Background(),
		db,
		"",
		"SELECT name FROM metrics",
		nil,
	)
	if err != nil {
		t.Fatalf("IndexBy() error = %v", err)
	}
	if result == nil {
		t.Fatalf("IndexBy() result was nil")
	}
	if result.GroupsWritten != 1 {
		t.Fatalf("IndexBy() GroupsWritten = %d, want 1", result.GroupsWritten)
	}
	if result.WarmupKey == "" {
		t.Fatalf("IndexBy() WarmupKey was empty")
	}
	if result.MarkerKey != "" {
		t.Fatalf("IndexBy() MarkerKey = %q, want empty", result.MarkerKey)
	}
	if writes != 1 {
		t.Fatalf("expected exactly one cache write, got %d", writes)
	}
}

func TestCacheIndexBy_MarkerWriteFailureDoesNotCountMarker(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE metrics (order_id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err = db.Exec(`INSERT INTO metrics(order_id, name) VALUES (7, 'alpha')`); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	var markerWrites int
	markerErr := errors.New("marker write failed")
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			if _, ok := binMap[columnBin]; ok {
				markerWrites++
				return markerErr
			}
			return nil
		},
	}

	result, err := aCache.IndexByWithResult(
		context.Background(),
		db,
		"order_id",
		"SELECT order_id, name FROM metrics",
		nil,
	)
	if !errors.Is(err, markerErr) {
		t.Fatalf("IndexBy() error = %v, want %v", err, markerErr)
	}
	if result == nil {
		t.Fatalf("IndexBy() result was nil")
	}
	if result.GroupsWritten != 1 {
		t.Fatalf("IndexBy() GroupsWritten = %d, want 1", result.GroupsWritten)
	}
	if result.WarmupKey == "" {
		t.Fatalf("IndexBy() WarmupKey was empty")
	}
	if result.MarkerKey == "" {
		t.Fatalf("IndexBy() MarkerKey was empty")
	}
	if markerWrites != 1 {
		t.Fatalf("expected one marker write attempt, got %d", markerWrites)
	}
}

func TestCacheIndexBy_LegacyCountIncludesMarkerForIndexedWarmup(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE metrics (order_id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err = db.Exec(`INSERT INTO metrics(order_id, name) VALUES (7, 'alpha')`); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			return nil
		},
	}

	count, err := aCache.IndexBy(
		context.Background(),
		db,
		"order_id",
		"SELECT order_id, name FROM metrics",
		nil,
	)
	if err != nil {
		t.Fatalf("IndexBy() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("IndexBy() count = %d, want 2", count)
	}
}

func TestCacheUpdateMetaFields_LoadsStoredFieldsFromWarmupMarker(t *testing.T) {
	fieldsJSON := `[{"ColumnName":"order_id","ColumnScanType":"int","ColumnDatabaseName":"INTEGER"}]`
	storedFieldsJSON := `[{"Name":"order_id","ColumnName":"order_id"}]`
	entry := &cache.Entry{}

	err := (&Cache{}).updateMetaFields(entry, nil, &RecordMatched{
		record: &as.Record{
			Bins: as.BinMap{
				fieldsBin:       fieldsJSON,
				storedFieldsBin: storedFieldsJSON,
			},
		},
	})
	if err != nil {
		t.Fatalf("updateMetaFields() error = %v", err)
	}

	if len(entry.Meta.Fields) != 1 {
		t.Fatalf("unexpected physical fields count %d", len(entry.Meta.Fields))
	}
	if len(entry.Meta.StoredFields) != 1 {
		t.Fatalf("unexpected stored fields count %d", len(entry.Meta.StoredFields))
	}
	if entry.Meta.StoredFields[0].Name != "order_id" {
		t.Fatalf("unexpected stored field %+v", entry.Meta.StoredFields[0])
	}
}

func TestCacheUpdateMetaFields_ToleratesMissingStoredFieldsBin(t *testing.T) {
	fieldsJSON := `[{"ColumnName":"order_id","ColumnScanType":"int","ColumnDatabaseName":"INTEGER"}]`
	entry := &cache.Entry{}

	err := (&Cache{}).updateMetaFields(entry, nil, &RecordMatched{
		record: &as.Record{
			Bins: as.BinMap{
				fieldsBin: fieldsJSON,
			},
		},
	})
	if err != nil {
		t.Fatalf("updateMetaFields() error = %v", err)
	}

	if len(entry.Meta.Fields) != 1 {
		t.Fatalf("unexpected physical fields count %d", len(entry.Meta.Fields))
	}
	if entry.Meta.StoredFields != nil {
		t.Fatalf("expected missing stored fields bin to leave meta empty, got %+v", entry.Meta.StoredFields)
	}
}

func TestCacheUpdateMetaFields_PrefersWarmupStoredFieldsWhenLazyRecordExists(t *testing.T) {
	lazyFieldsJSON := `[{"ColumnName":"order_id","ColumnScanType":"int","ColumnDatabaseName":"INTEGER"}]`
	warmupFieldsJSON := `[{"ColumnName":"campaign_id","ColumnScanType":"int","ColumnDatabaseName":"INTEGER"}]`
	storedFieldsJSON := `[{"Name":"campaign_id","ColumnName":"campaign_id"}]`
	entry := &cache.Entry{}

	err := (&Cache{}).updateMetaFields(
		entry,
		&RecordMatched{
			record: &as.Record{
				Bins: as.BinMap{
					fieldsBin: lazyFieldsJSON,
				},
			},
		},
		&RecordMatched{
			record: &as.Record{
				Bins: as.BinMap{
					fieldsBin:       warmupFieldsJSON,
					storedFieldsBin: storedFieldsJSON,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("updateMetaFields() error = %v", err)
	}

	if len(entry.Meta.Fields) != 1 || entry.Meta.Fields[0].ColumnName != "order_id" {
		t.Fatalf("expected physical fields to stay sourced from lazy record, got %+v", entry.Meta.Fields)
	}
	if len(entry.Meta.StoredFields) != 1 {
		t.Fatalf("unexpected stored fields count %d", len(entry.Meta.StoredFields))
	}
	if entry.Meta.StoredFields[0].Name != "campaign_id" {
		t.Fatalf("unexpected stored field %+v", entry.Meta.StoredFields[0])
	}
}

func TestWarmupProjectionIndexes_NonGroupableSubset(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "order_id", FieldName: "OrderId", ColumnName: "order_id", Lookup: []string{"order_id", "orderid"}},
		{Name: "bids", FieldName: "Bids", ColumnName: "bids", Lookup: []string{"bids"}},
		{Name: "impressions", FieldName: "Impressions", ColumnName: "impressions", Lookup: []string{"impressions"}},
	}
	requested := []cache.ProjectionField{
		{Name: "impressions"},
		{Name: "order_id"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected projection to be compatible")
	}
	if got, want := indexes, []int{2, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_MatchesCanonicalProjectionKeys(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{MeasureKey: "metrics.spend"},
		{DimensionKey: "campaign.id"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected canonical projection keys to match")
	}
	if got, want := indexes, []int{1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_NonGroupedCanonicalKeysStillUseSubsetLogic(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
		{Name: "Clicks", MeasureKey: "metrics.clicks"},
	}
	requested := []cache.ProjectionField{
		{MeasureKey: "metrics.clicks"},
		{DimensionKey: "campaign.id"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected non-grouped canonical-key subset to stay compatible")
	}
	if got, want := indexes, []int{2, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_RejectsAmbiguousStoredAliases(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id"},
		{Name: "campaignid"},
	}
	requested := []cache.ProjectionField{{Name: "campaign_id"}}

	_, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected ambiguous stored aliases to be rejected")
	}
}

func TestWarmupProjectionIndexes_IgnoresRequestedLookupWhenCanonicalIdentityIsUnique(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id"},
		{Name: "order_id", Lookup: []string{"campaignid"}},
	}
	requested := []cache.ProjectionField{{
		Name:   "campaign_id",
		Lookup: []string{"order_id"},
	}}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected canonical identity to win over conflicting requested lookup aliases")
	}
	if got, want := indexes, []int{0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_AllowsExactShapeWithDuplicateSource(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id", FieldName: "CampaignId", Source: "ID"},
		{Name: "media_plan_id", FieldName: "MediaPlanId", Source: "ID"},
		{Name: "spend", FieldName: "Spend", Source: "SPEND"},
	}
	requested := []cache.ProjectionField{
		{Name: "campaign_id", FieldName: "CampaignId", Source: "ID"},
		{Name: "media_plan_id", FieldName: "MediaPlanId", Source: "ID"},
		{Name: "spend", FieldName: "Spend", Source: "SPEND"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected exact-shape projection to be compatible")
	}
	if got, want := indexes, []int{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_IgnoresDuplicateSourceWhenPrimaryIdentityIsUnique(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id", FieldName: "CampaignId", Source: "ID"},
		{Name: "media_plan_id", FieldName: "MediaPlanId", Source: "ID"},
		{Name: "spend", FieldName: "Spend", Source: "SPEND"},
	}
	requested := []cache.ProjectionField{
		{Name: "media_plan_id", FieldName: "MediaPlanId"},
		{Name: "campaign_id", FieldName: "CampaignId"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected unique output identities to match despite duplicate source aliases")
	}
	if got, want := indexes, []int{1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_UsesUniqueSourceAsWeakFallback(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id", FieldName: "CampaignId", Source: "CAMPAIGN_ID"},
		{Name: "media_plan_id", FieldName: "MediaPlanId", Source: "MEDIA_PLAN_ID"},
	}
	requested := []cache.ProjectionField{
		{Source: "MEDIA_PLAN_ID"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected unique source fallback to match")
	}
	if got, want := indexes, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_RejectsDuplicateSourceOnlyWhenNoStrongIdentityMatches(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "campaign_id", FieldName: "CampaignId", Source: "ID"},
		{Name: "media_plan_id", FieldName: "MediaPlanId", Source: "ID"},
	}
	requested := []cache.ProjectionField{
		{Source: "ID"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected duplicate source fallback to be rejected")
	}
	if reason != "non_grouped_ambiguous_stored_alias" {
		t.Fatalf("unexpected reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_AllowsExactShapeWithDuplicateLookupAliases(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "CAMPAIGN_ID", FieldName: "CAMPAIGN_ID", ColumnName: "CAMPAIGN_ID", Source: "ID", Lookup: []string{"CAMPAIGN_ID", "campaignid", "campaign_id", "ID", "id"}},
		{Name: "MEDIA_PLAN_ID", FieldName: "MEDIA_PLAN_ID", ColumnName: "MEDIA_PLAN_ID", Source: "ID", Lookup: []string{"MEDIA_PLAN_ID", "mediaplanid", "media_plan_id", "ID", "id"}},
	}
	requested := []cache.ProjectionField{
		{Name: "CAMPAIGN_ID", FieldName: "CAMPAIGN_ID", ColumnName: "CAMPAIGN_ID", Source: "ID", Lookup: []string{"CAMPAIGN_ID", "campaignid", "campaign_id", "ID", "id"}},
		{Name: "MEDIA_PLAN_ID", FieldName: "MEDIA_PLAN_ID", ColumnName: "MEDIA_PLAN_ID", Source: "ID", Lookup: []string{"MEDIA_PLAN_ID", "mediaplanid", "media_plan_id", "ID", "id"}},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected exact-shape projection to be compatible despite duplicate weak lookup aliases")
	}
	if got, want := indexes, []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_IgnoresDuplicateLookupAliasesWhenStrongIdentityIsUnique(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "CAMPAIGN_ID", FieldName: "CAMPAIGN_ID", ColumnName: "CAMPAIGN_ID", Source: "ID", Lookup: []string{"CAMPAIGN_ID", "campaignid", "campaign_id", "ID", "id"}},
		{Name: "MEDIA_PLAN_ID", FieldName: "MEDIA_PLAN_ID", ColumnName: "MEDIA_PLAN_ID", Source: "ID", Lookup: []string{"MEDIA_PLAN_ID", "mediaplanid", "media_plan_id", "ID", "id"}},
	}
	requested := []cache.ProjectionField{
		{Name: "MEDIA_PLAN_ID", FieldName: "MEDIA_PLAN_ID", ColumnName: "MEDIA_PLAN_ID", Lookup: []string{"MEDIA_PLAN_ID", "mediaplanid", "media_plan_id", "ID", "id"}},
		{Name: "CAMPAIGN_ID", FieldName: "CAMPAIGN_ID", ColumnName: "CAMPAIGN_ID", Lookup: []string{"CAMPAIGN_ID", "campaignid", "campaign_id", "ID", "id"}},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected unique strong identities to match despite duplicate weak lookup aliases")
	}
	if got, want := indexes, []int{1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_RejectsDuplicateLookupAliasesWhenNoCanonicalIdentityExists(t *testing.T) {
	stored := []cache.ProjectionField{
		{Source: "ID", Lookup: []string{"ID", "id"}},
		{Source: "ID", Lookup: []string{"ID", "id"}},
	}
	requested := []cache.ProjectionField{
		{Source: "ID", Lookup: []string{"ID", "id"}},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected duplicate source fallback without canonical identity to be rejected")
	}
	if reason != "non_grouped_ambiguous_stored_alias" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_GroupedAllowsSameDimensionsAndSubsetMeasures(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Day", DimensionKey: "date.day"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
		{Name: "Clicks", MeasureKey: "metrics.clicks"},
	}
	requested := []cache.ProjectionField{
		{Name: "Day", DimensionKey: "date.day"},
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Clicks", MeasureKey: "metrics.clicks"},
	}

	indexes, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if !ok {
		t.Fatalf("expected grouped projection to be compatible")
	}
	if got, want := indexes, []int{1, 0, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected indexes %v, want %v", got, want)
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsDifferentDimensions(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Day", DimensionKey: "date.day"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Hour", DimensionKey: "date.hour"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}

	_, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped projection with different dimensions to be rejected")
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsDuplicateRequestedDimensions(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Day", DimensionKey: "date.day"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "CampaignAgain", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped projection with duplicate requested dimensions to be rejected")
	}
	if reason != "grouped_duplicate_dimension" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsDuplicateStoredDimensions(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "CampaignAgain", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}

	_, ok, _, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped projection with duplicate stored dimensions to be rejected")
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsMissingMeasureSubset(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Clicks", MeasureKey: "metrics.clicks"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped projection with missing measure to be rejected")
	}
	if reason != "grouped_missing_measure" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsMissingCanonicalKeys(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
		{Name: "Spend", MeasureKey: "metrics.spend"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped projection without canonical keys to be rejected")
	}
	if reason != "grouped_invalid_metadata" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_GroupedRejectsHybridDimensionMeasureField(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id", MeasureKey: "metrics.campaign"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected grouped hybrid field to be rejected")
	}
	if reason != "grouped_invalid_metadata" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestWarmupProjectionIndexes_GroupedExactShapeStillRejectsInvalidMetadata(t *testing.T) {
	stored := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id", MeasureKey: "metrics.campaign"},
	}
	requested := []cache.ProjectionField{
		{Name: "Campaign", DimensionKey: "campaign.id", MeasureKey: "metrics.campaign"},
	}

	_, ok, reason, err := warmupProjectionIndexes(stored, requested)
	if err != nil {
		t.Fatalf("warmupProjectionIndexes() error = %v", err)
	}
	if ok {
		t.Fatalf("expected exact-shape grouped invalid metadata to be rejected")
	}
	if reason != "grouped_invalid_metadata" {
		t.Fatalf("unexpected reject reason %q", reason)
	}
}

func TestCacheApplyWarmupProjection_ClearsWarmupMetadataOnIncompatibleProjection(t *testing.T) {
	entry := &cache.Entry{
		Meta: cache.Meta{
			Fields: []*cache.Field{
				{ColumnName: "order_id", ColumnScanType: "int"},
				{ColumnName: "bids", ColumnScanType: "int"},
			},
			Type: []string{"int", "int"},
			StoredFields: []cache.ProjectionField{
				{Name: "order_id"},
				{Name: "bids"},
			},
		},
	}
	matcher := &cache.ParmetrizedQuery{
		RequestedFields: []cache.ProjectionField{{Name: "clicks"}},
	}
	stats := &cache.Stats{Type: cache.TypeReadMulti, FoundWarmup: true, RecordsCounter: 1}

	err := (&Cache{}).applyWarmupProjection(entry, matcher, stats)
	if err != nil {
		t.Fatalf("applyWarmupProjection() error = %v", err)
	}

	if stats.Type != cache.TypeNone {
		t.Fatalf("expected stats type %q, got %q", cache.TypeNone, stats.Type)
	}
	if stats.FoundWarmup {
		t.Fatalf("expected warmup flag to be cleared")
	}
	if entry.Meta.Fields != nil || entry.Meta.StoredFields != nil || entry.Meta.Type != nil || entry.Meta.ProjectedIndexes != nil {
		t.Fatalf("expected warmup metadata to be cleared, got %+v", entry.Meta)
	}
}

func TestCacheReadRecords_PopulatesWarmupStatsOnMarkerMiss(t *testing.T) {
	aCache := &Cache{namespace: "ns_memory", set: "steward_test"}
	matcher := &cache.ParmetrizedQuery{
		By:  "campaign_id",
		SQL: "SELECT campaign_id, name FROM campaign WHERE tenant_id = ?",
		Args: []interface{}{
			"tenant-a",
		},
	}
	stats := &cache.Stats{}
	aCache.getRecordFn = func(key *as.Key, bins ...string) (*as.Record, error) {
		return nil, types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
	}

	_, _, err := aCache.readRecords("SELECT * FROM lazy WHERE id = ?", []interface{}{1}, matcher, stats)
	if err != nil {
		t.Fatalf("readRecords() error = %v", err)
	}
	if stats.WarmupKey == "" {
		t.Fatalf("expected WarmupKey to be populated")
	}
	if stats.MarkerKey == "" {
		t.Fatalf("expected MarkerKey to be populated")
	}
}

func TestCacheReadRecords_PopulatesWarmupStatsOnMarkerHit(t *testing.T) {
	aCache := &Cache{namespace: "ns_memory", set: "steward_test"}
	matcher := &cache.ParmetrizedQuery{
		By:  "campaign_id",
		SQL: "SELECT campaign_id, name FROM campaign WHERE tenant_id = ?",
		Args: []interface{}{
			"tenant-a",
		},
	}
	stats := &cache.Stats{}

	identitySQL, _, identityArgsMarshal, err := matcher.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}
	identitySQL, identityArgsMarshal, _ = canonicalWarmupIdentity(identitySQL, identityArgsMarshal)
	warmupKey, err := aCache.identityURL(identitySQL, nil, identityArgsMarshal)
	if err != nil {
		t.Fatalf("identityURL() error = %v", err)
	}
	markerKey := aCache.columnURL(warmupKey, matcher.By)
	markerStoreKey, err := aCache.key(markerKey)
	if err != nil {
		t.Fatalf("key(%q) error = %v", markerKey, err)
	}
	aCache.getRecordFn = func(key *as.Key, bins ...string) (*as.Record, error) {
		if key.String() == markerStoreKey.String() {
			return &as.Record{
				Bins: as.BinMap{
					sqlBin:  identitySQL,
					argsBin: string(identityArgsMarshal),
				},
			}, nil
		}
		return nil, types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
	}

	_, _, err = aCache.readRecords("SELECT * FROM lazy WHERE id = ?", []interface{}{1}, matcher, stats)
	if err != nil {
		t.Fatalf("readRecords() error = %v", err)
	}
	if stats.WarmupKey != warmupKey {
		t.Fatalf("expected WarmupKey %q, got %q", warmupKey, stats.WarmupKey)
	}
	if stats.MarkerKey != markerKey {
		t.Fatalf("expected MarkerKey %q, got %q", markerKey, stats.MarkerKey)
	}
}
