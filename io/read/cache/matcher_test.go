package cache

import (
	"encoding/json"
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
