package cache

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParmetrizedQueryWarmupIdentity_DefaultsToExecutionSQLAndArgs(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:  "SELECT * FROM entity_relation WHERE scope_id = ?",
		Args: []interface{}{"scope-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.SQL {
		t.Fatalf("expected identity SQL %q, got %q", query.SQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "scope-a" {
		t.Fatalf("expected identity args [scope-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"scope-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_UsesExplicitIdentity(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:          "SELECT * FROM entity_relation WHERE scope_id = ? AND group_id = ?",
		Args:         []interface{}{"scope-a", 2002},
		IdentitySQL:  "SELECT * FROM entity_relation WHERE scope_id = ?",
		IdentityArgs: []interface{}{"scope-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if identitySQL != query.IdentitySQL {
		t.Fatalf("expected identity SQL %q, got %q", query.IdentitySQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "scope-a" {
		t.Fatalf("expected identity args [scope-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"scope-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromSimpleByPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "group_id",
		SQL:  "SELECT * FROM entity_relation WHERE scope_id = ? AND group_id = ?",
		Args: []interface{}{"scope-a", 2002},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM entity_relation WHERE scope_id = ?"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	if len(identityArgs) != 1 || identityArgs[0] != "scope-a" {
		t.Fatalf("expected identity args [scope-a], got %v", identityArgs)
	}

	wantMarshal, err := json.Marshal([]interface{}{"scope-a"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(identityArgsMarshal) != string(wantMarshal) {
		t.Fatalf("expected identity args marshal %s, got %s", string(wantMarshal), string(identityArgsMarshal))
	}
}

func TestParmetrizedQueryWarmupIdentity_DerivesWarmupIdentityFromSimpleByInPredicate(t *testing.T) {
	query := &ParmetrizedQuery{
		By:   "item_id",
		SQL:  "SELECT * FROM scoped_items WHERE scope_id = ? AND event_day BETWEEN ? AND ? AND item_id IN (?, ?, ?)",
		Args: []interface{}{"scope-a", "2026-07-01", "2026-07-31", 101, 202, 303},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM scoped_items WHERE scope_id = ? AND event_day BETWEEN ? AND ?"
	if normalizeSQL(identitySQL) != normalizeSQL(wantSQL) {
		t.Fatalf("expected identity SQL %q, got %q", wantSQL, identitySQL)
	}
	wantArgs := []interface{}{"scope-a", "2026-07-01", "2026-07-31"}
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
		By:   "entity_id",
		SQL:  "SELECT * FROM activity_metrics p WHERE p.entity_id IS NOT NULL AND business_date = CURRENT_DATE() AND p.entity_id IN (?)",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM activity_metrics p WHERE p.entity_id IS NOT NULL AND business_date = CURRENT_DATE()"
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
		By: "entity_id",
		SQL: "SELECT * FROM activity_metrics p " +
			"WHERE p.entity_id IS NOT NULL AND (p.views > 0 OR p.actions > 0) AND p.entity_id IN (?)",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	wantSQL := "SELECT * FROM activity_metrics p WHERE p.entity_id IS NOT NULL AND (p.views > 0 OR p.actions > 0)"
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
		By: "entity_id",
		SQL: "SELECT t.report_time, t.entity_id, t.VISIBLE_COUNT, t.MEASURED_COUNT FROM (" +
			"SELECT TIMESTAMP_TRUNC(i.report_time, DAY) AS report_time, i.entity_id, " +
			"SUM(COALESCE(i.visible_count, 0)) AS VISIBLE_COUNT, " +
			"SUM(COALESCE(i.measured_count, 0)) AS MEASURED_COUNT " +
			"FROM project.dataset.daily_measurements i " +
			"WHERE i.entity_id IS NOT NULL " +
			"AND (((business_date = CURRENT_DATE() AND source_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))) " +
			"AND (i.entity_id IN (?)) " +
			"GROUP BY 1, 2 ORDER BY report_time LIMIT 1000) AS t",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "i.entity_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 1000") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "i.entity_id IS NOT NULL") {
		t.Fatalf("expected non-selector predicate to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "business_date = CURRENT_DATE()") {
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
		By: "entity_id",
		SQL: "SELECT t.entity_id, t.report_time, t.TIME_RATIO, t.EXPECTED_RATE, " +
			"t.DAILY_LIMIT, t.PACING_INDEX, t.ACHIEVED_VALUE, t.CURRENT_VALUE, " +
			"t.DAILY_TARGET, t.TOTAL_TARGET FROM (" +
			"WITH ranked AS (" +
			"SELECT id AS entity_id, " +
			"DATETIME_TRUNC(DATETIME(snapshot_time, timezone_name), DAY) AS report_time, " +
			"DATE(DATETIME(snapshot_time, timezone_name)) AS business_date, " +
			"DATE(snapshot_time) AS source_date, " +
			"pctElapsed AS TIME_RATIO, " +
			"expectedRate AS EXPECTED_RATE, " +
			"dailyLimit AS DAILY_LIMIT, " +
			"cycleMetrics.pacingIndex AS PACING_INDEX, " +
			"delivery.currentTotals.totalAmount AS ACHIEVED_VALUE, " +
			"delivery.currentTotals.totalAmount AS CURRENT_VALUE, " +
			"constraints.daily.target AS DAILY_TARGET, " +
			"constraints.total.target AS TOTAL_TARGET, " +
			"snapshot_time, " +
			"ROW_NUMBER() OVER (PARTITION BY id, DATETIME_TRUNC(DATETIME(snapshot_time, timezone_name), DAY) ORDER BY snapshot_time DESC) AS RN " +
			"FROM project.dataset.fulfillment_snapshots" +
			") " +
			"SELECT entity_id, report_time, TIME_RATIO, EXPECTED_RATE, DAILY_LIMIT, " +
			"PACING_INDEX, ACHIEVED_VALUE, CURRENT_VALUE, DAILY_TARGET, TOTAL_TARGET " +
			"FROM ranked WHERE RN = 1 AND entity_id IS NOT NULL " +
			"AND (((business_date = CURRENT_DATE() AND source_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))) " +
			"AND (entity_id IN (?)) ORDER BY report_time LIMIT 1000) AS t",
		Args: []interface{}{2684543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "entity_id IN (?)") {
		t.Fatalf("expected selector predicate to be removed, got %q", identitySQL)
	}
	if strings.Contains(strings.ToUpper(identitySQL), "LIMIT 1000") {
		t.Fatalf("expected limit to be removed, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "project.dataset.fulfillment_snapshots") {
		t.Fatalf("expected pacing source query to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "RN = 1") {
		t.Fatalf("expected final-select predicate to remain, got %q", identitySQL)
	}
	if !strings.Contains(identitySQL, "entity_id IS NOT NULL") {
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
		By: "entity_id",
		SQL: "SELECT * FROM activity_metrics p " +
			"WHERE business_date = CURRENT_DATE() AND p.entity_id IN (?) AND p.entity_id = ?",
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
		By: "segment_id",
		SQL: "SELECT t.event_date, t.entity_id, t.segment_id FROM (" +
			"SELECT si.event_date, si.entity_id, si.segment_id " +
			"FROM dataset.nested_rejections si " +
			"JOIN UNNEST(si.reason_estimates) fr ON 1=1 " +
			"WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND (si.segment_id IN (?)) " +
			"LIMIT 40) AS t",
		Args: []interface{}{7333543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "si.segment_id IN (?)") {
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
		By: "segment_id",
		SQL: "SELECT t.event_date, t.entity_id, t.segment_id FROM (" +
			"SELECT si.event_date, si.entity_id, si.segment_id " +
			"FROM dataset.nested_rejections si " +
			"WHERE si.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND (si.segment_id IN (?))) AS t " +
			"ORDER BY t.event_date LIMIT 40 OFFSET 10",
		Args: []interface{}{7333543},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := query.WarmupIdentity()
	if err != nil {
		t.Fatalf("WarmupIdentity() error = %v", err)
	}

	if strings.Contains(identitySQL, "si.segment_id IN (?)") {
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
		By: "segment_id",
		SQL: "SELECT t.event_date, ROW_NUMBER() OVER win AS rn FROM (" +
			"SELECT si.event_date, si.segment_id " +
			"FROM dataset.nested_rejections si " +
			"WHERE si.segment_id IN (?)) AS t " +
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
		By: "GROUP_ID",
		SQL: "SELECT t.GROUP_ID, t.NAME FROM (" +
			"SELECT c.ID AS GROUP_ID, c.NAME " +
			"FROM ENTITY c " +
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
		By: "GROUP_ID",
		SQL: "SELECT t.GROUP_ID FROM (" +
			"SELECT c.ID AS GROUP_ID " +
			"FROM ENTITY c JOIN ENTITY_RELATION rel ON 1 = 1 " +
			"WHERE rel.ID IN (?) " +
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
		By:   "group_id",
		SQL:  "SELECT * FROM entity_relation WHERE scope_id = ? AND (group_id = ? OR group_id = ?)",
		Args: []interface{}{"scope-a", 100, 200},
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
		By:   "group_id",
		SQL:  "SELECT ? AS marker FROM entity_relation WHERE scope_id = ? AND group_id = ?",
		Args: []interface{}{"warmup", "scope-a", 2002},
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
		SQL:          "SELECT * FROM entity_relation",
		IdentityArgs: []interface{}{"scope-a"},
	}

	_, _, _, err := query.WarmupIdentity()
	if err == nil {
		t.Fatalf("expected WarmupIdentity() to reject partial identity")
	}
}

func TestParmetrizedQueryWarmupIdentity_NormalizesNilIdentityArgs(t *testing.T) {
	query := &ParmetrizedQuery{
		SQL:         "SELECT * FROM entity_relation WHERE scope_id = ?",
		Args:        []interface{}{"scope-a"},
		IdentitySQL: "SELECT * FROM entity_relation",
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
		SQL: "SELECT * FROM entity_relation",
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
