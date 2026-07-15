package cache

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParmetrizedQueryWarmupIdentity_DefaultsToExecutionSQLAndArgs(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ?",
		Args: []interface{}{"tenant-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "tenant-a" {
		t.Fatalf("expected identity args [tenant-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"tenant-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_UsesExplicitIdentity(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		Args:         []interface{}{"tenant-a", 2002},
		IdentitySQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ?",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.IdentitySQL {
		t.Fatalf("expected identity SQL %q, got %q", query.IdentitySQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "tenant-a" {
		t.Fatalf("expected identity args [tenant-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"tenant-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromSimpleByPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "campaign_id",
		SQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		Args: []interface{}{"tenant-a", 2002},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM campaign_flight WHERE tenant_id = ?"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "tenant-a" {
		t.Fatalf("expected identity args [tenant-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"tenant-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromSimpleByInPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "ad_order_id",
		SQL:  "SELECT * FROM ad_order_flight WHERE tenant_id = ? AND event_day BETWEEN ? AND ? AND ad_order_id IN (?, ?, ?)",
		Args: []interface{}{"tenant-a", "2026-07-01", "2026-07-31", 101, 202, 303},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM ad_order_flight WHERE tenant_id = ? AND event_day BETWEEN ? AND ?"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	wantArgs := []interface{}{"tenant-a", "2026-07-01", "2026-07-31"}
	if len(identityArgs) != len(wantArgs) {
		t.Fatalf("expected identity args %v, got %v", wantArgs, identityArgs)
	}
	for i := range wantArgs {
		if identityArgs[i] != wantArgs[i] {
			t.Fatalf("expected identity args %v, got %v", wantArgs, identityArgs)
		}
	}

	wantMarshal, err := json.Marshal(wantArgs)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_IgnoresByIsNotNullAndRemovesByInPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "order_id",
		SQL:  "SELECT * FROM fact_perf_daily_v p WHERE p.order_id IS NOT NULL AND advertiser_date = CURRENT_DATE() AND p.order_id IN (?)",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM fact_perf_daily_v p WHERE p.order_id IS NOT NULL AND advertiser_date = CURRENT_DATE()"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_PreservesGroupedOrPredicateWhenRemovingSelector(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "order_id",
		SQL: "SELECT * FROM fact_perf_daily_v p " +
			"WHERE p.order_id IS NOT NULL AND (p.impressions > 0 OR p.clicks > 0) AND p.order_id IN (?)",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM fact_perf_daily_v p WHERE p.order_id IS NOT NULL AND (p.impressions > 0 OR p.clicks > 0)"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromGroupedAndTimelinePredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "order_id",
		SQL: "SELECT t.advertiser_time, t.order_id, t.IAS_IN_VIEW_IMPS, t.IAS_MEASURED_IMPS FROM (" +
			"SELECT TIMESTAMP_TRUNC(i.advertiser_time, DAY) AS advertiser_time, i.order_id, " +
			"SUM(COALESCE(i.ias_in_view_imps, 0)) AS IAS_IN_VIEW_IMPS, " +
			"SUM(COALESCE(i.ias_measured_imps, 0)) AS IAS_MEASURED_IMPS " +
			"FROM viant-mediator.steward.fact_ias_daily_v i " +
			"WHERE i.order_id IS NOT NULL " +
			"AND (((advertiser_date = CURRENT_DATE() AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))) " +
			"AND (i.order_id IN (?)) " +
			"GROUP BY 1, 2 ORDER BY advertiser_time LIMIT 1000) AS t",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "i.order_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 1000") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "i.order_id IS NOT NULL") {
		t.Fatalf("expected non-selector predicate to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "advertiser_date = CURRENT_DATE()") {
		t.Fatalf("expected grouped date predicate to remain, got %q", identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromNestedCTEByInPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "order_id",
		SQL: "SELECT t.order_id, t.advertiser_time, t.TIME_ELAPSED_PCT, t.EXPECTED_PACING_RATE, " +
			"t.PACE_DAILY_BUDGET, t.BUDGET_PACING_INDEX, t.ACHIEVED_SPEND, t.TODAY_SPEND, " +
			"t.DAILY_BUDGET, t.LIFETIME_BUDGET FROM (" +
			"WITH ranked AS (" +
			"SELECT id AS order_id, " +
			"DATETIME_TRUNC(DATETIME(allocationTime, ianaTimezoneStr), DAY) AS advertiser_time, " +
			"DATE(DATETIME(allocationTime, ianaTimezoneStr)) AS advertiser_date, " +
			"DATE(allocationTime) AS event_date, " +
			"pctServingTimeElapsed AS TIME_ELAPSED_PCT, " +
			"expectedPacingRate AS EXPECTED_PACING_RATE, " +
			"paceDailyBudget AS PACE_DAILY_BUDGET, " +
			"cyclePerformance.budgetPacingIndex AS BUDGET_PACING_INDEX, " +
			"delivery.todaySpendTotal.totalSpendLocal AS ACHIEVED_SPEND, " +
			"delivery.todaySpendTotal.totalSpendLocal AS TODAY_SPEND, " +
			"constraints.daily.budget AS DAILY_BUDGET, " +
			"constraints.lifetime.budget AS LIFETIME_BUDGET, " +
			"allocationTime, " +
			"ROW_NUMBER() OVER (PARTITION BY id, DATETIME_TRUNC(DATETIME(allocationTime, ianaTimezoneStr), DAY) ORDER BY allocationTime DESC) AS RN " +
			"FROM viant-mediator.mdp.bidalloc_adorder_fulfillment" +
			") " +
			"SELECT order_id, advertiser_time, TIME_ELAPSED_PCT, EXPECTED_PACING_RATE, PACE_DAILY_BUDGET, " +
			"BUDGET_PACING_INDEX, ACHIEVED_SPEND, TODAY_SPEND, DAILY_BUDGET, LIFETIME_BUDGET " +
			"FROM ranked WHERE RN = 1 AND order_id IS NOT NULL " +
			"AND (((advertiser_date = CURRENT_DATE() AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))) " +
			"AND (order_id IN (?)) ORDER BY advertiser_time LIMIT 1000) AS t",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "order_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 1000") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "viant-mediator.mdp.bidalloc_adorder_fulfillment") {
		t.Fatalf("expected pacing source query to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "RN = 1") {
		t.Fatalf("expected final-select predicate to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "order_id IS NOT NULL") {
		t.Fatalf("expected non-selector predicate to remain, got %q", identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_FallsBackWhenMultipleByInPredicatesExist(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "order_id",
		SQL: "SELECT * FROM fact_perf_daily_v p " +
			"WHERE advertiser_date = CURRENT_DATE() AND p.order_id IN (?) AND p.order_id = ?",
		Args: []interface{}{2684543, 2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected fallback identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != len(query.Args) {
		t.Fatalf("expected fallback identity args %v, got %v", query.Args, identityArgs)
	}

	wantMarshal, err := json.Marshal(query.Args)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected fallback identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromNestedByInPredicateAndDropsLimit(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "audience_id",
		SQL: "SELECT t.event_date, t.order_id, t.audience_id FROM (" +
			"SELECT si.event_date, si.order_id, si.audience_id " +
			"FROM dataset.soft_ineligibilities si " +
			"JOIN UNNEST(si.feature_rejection_estimates) fr ON 1=1 " +
			"WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND (si.audience_id IN (?)) " +
			"LIMIT 40) AS t",
		Args: []interface{}{7333543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "si.audience_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 40") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)") {
		t.Fatalf("expected non-selector predicate to remain, got %q", identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromPureWrapperAndDropsOuterPagination(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "audience_id",
		SQL: "SELECT t.event_date, t.order_id, t.audience_id FROM (" +
			"SELECT si.event_date, si.order_id, si.audience_id " +
			"FROM dataset.soft_ineligibilities si " +
			"WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND (si.audience_id IN (?))) AS t " +
			"ORDER BY t.event_date LIMIT 40 OFFSET 10",
		Args: []interface{}{7333543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "si.audience_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 40") {
		t.Fatalf("expected outer limit to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "OFFSET 10") {
		t.Fatalf("expected outer offset to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "ORDER BY T.EVENT_DATE") {
		t.Fatalf("expected outer order by to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)") {
		t.Fatalf("expected inner non-selector predicate to remain, got %q", identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_FallsBackWhenOuterWrapperUsesRealWindowClause(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "audience_id",
		SQL: "SELECT t.event_date, ROW_NUMBER() OVER win AS rn FROM (" +
			"SELECT si.event_date, si.audience_id " +
			"FROM dataset.soft_ineligibilities si " +
			"WHERE si.audience_id IN (?)) AS t " +
			"WINDOW win AS (ORDER BY t.event_date)",
		Args: []interface{}{7333543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected fallback identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != len(query.Args) || identityArgs[0] != query.Args[0] {
		t.Fatalf("expected fallback identity args %v, got %v", query.Args, identityArgs)
	}
	wantMarshal, err := json.Marshal(query.Args)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected fallback identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromProjectionAliasSelector(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "CAMPAIGN_ID",
		SQL: "SELECT t.CAMPAIGN_ID, t.NAME FROM (" +
			"SELECT c.ID AS CAMPAIGN_ID, c.NAME " +
			"FROM CI_CAMPAIGN c " +
			"WHERE 1 = 1 AND (c.ID IN (?)) " +
			"LIMIT 40) t",
		Args: []interface{}{556110},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "c.ID IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 40") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "WHERE 1 = 1") {
		t.Fatalf("expected non-selector predicate to remain, got %q", identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected empty identity args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected empty identity args marshal [], got %s", string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DoesNotMatchUnrelatedQualifiedLeafFromProjectionAlias(t *testing.T) {
	query := &ParmetrizedQuery{
		By: "CAMPAIGN_ID",
		SQL: "SELECT t.CAMPAIGN_ID FROM (" +
			"SELECT c.ID AS CAMPAIGN_ID " +
			"FROM CI_CAMPAIGN c JOIN CI_CAMPAIGN_FLIGHT cf ON 1 = 1 " +
			"WHERE cf.ID IN (?) " +
			"LIMIT 40) t",
		Args: []interface{}{556110},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected fallback identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != len(query.Args) || identityArgs[0] != query.Args[0] {
		t.Fatalf("expected fallback identity args %v, got %v", query.Args, identityArgs)
	}
	wantMarshal, err := json.Marshal(query.Args)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected fallback identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_FallsBackWhenSelectorPredicateIsNotTopLevelAnd(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "campaign_id",
		SQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ? AND (campaign_id = ? OR campaign_id = ?)",
		Args: []interface{}{"tenant-a", 100, 200},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected fallback identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != len(query.Args) {
		t.Fatalf("expected fallback identity args %v, got %v", query.Args, identityArgs)
	}
	wantMarshal, err := json.Marshal(query.Args)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected fallback identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_FallsBackWhenQueryHasPlaceholderOutsideWhere(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "campaign_id",
		SQL:  "SELECT ? AS marker FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		Args: []interface{}{"warmup", "tenant-a", 2002},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected fallback identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != len(query.Args) {
		t.Fatalf("expected fallback identity args %v, got %v", query.Args, identityArgs)
	}
	wantMarshal, err := json.Marshal(query.Args)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected fallback identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_RejectsPartialIdentity(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	_, _, _, err := query.WarmupIdentity()
	if err == nil {
		t.Fatalf("expected WarmupIdentity() to reject partial identity")
	}
}

func TestParmetrizedQueryWarmupIdentity_NormalizesNilIdentityArgs(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:         "SELECT * FROM campaign_flight WHERE tenant_id = ?",
		Args:        []interface{}{"tenant-a"},
		IdentitySQL: "SELECT * FROM campaign_flight",
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.IdentitySQL {
		t.Fatalf("expected identity SQL %q, got %q", query.IdentitySQL, identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected normalized empty identity args, got %v", identityArgs)
	}

	wantMarshal := "[]"
	if string(identityArgsMarshal) != wantMarshal {
		t.Fatalf("expected normalized empty identity args marshal %s, got %s", wantMarshal, string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_NormalizesEmptyExecutionArgs(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL: "SELECT * FROM campaign_flight",
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != 0 {
		t.Fatalf("expected normalized empty execution args, got %v", identityArgs)
	}
	if string(identityArgsMarshal) != "[]" {
		t.Fatalf("expected normalized empty execution args marshal [], got %s", string(identityArgsMarshal))
	}
}

func normalizeSQL(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
