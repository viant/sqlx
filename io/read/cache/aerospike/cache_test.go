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

	identitySQL, identityArgs, identityArgsMarshal, err := aCache.resolveIndexIdentity(
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
}

func TestCacheResolveIndexIdentity_UsesMatcherWarmupIdentity(t *testing.T) {
	aCache := &Cache{}
	matcher := &cache.ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight WHERE tenant_id = ? AND campaign_id = ?",
		Args:         []interface{}{"tenant-a", 2002},
		IdentitySQL:  "SELECT * FROM campaign_flight WHERE tenant_id = ?",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	identitySQL, identityArgs, identityArgsMarshal, err := aCache.resolveIndexIdentity(
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
}

func TestCacheResolveIndexIdentity_RejectsInvalidMatcherIdentity(t *testing.T) {
	aCache := &Cache{}
	matcher := &cache.ParmetrizedQuery{
		SQL:          "SELECT * FROM campaign_flight",
		IdentityArgs: []interface{}{"tenant-a"},
	}

	_, _, _, err := aCache.resolveIndexIdentity(matcher.SQL, matcher.Args, matcher)
	if err == nil {
		t.Fatalf("expected resolveIndexIdentity() to reject invalid matcher identity")
	}
}
