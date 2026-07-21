package aerospike

import (
	"github.com/viant/sqlx/io/read/cache"
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
