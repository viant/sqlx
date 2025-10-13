package insert

import (
	"context"
	"testing"

	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
)

func TestNew_UseMetaSessionCacheOption(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name   string
		opts   []option.Option
		expect bool
	}{
		{
			name:   "default (no option provided)",
			opts:   nil,
			expect: false, // current default in service.New
		},
		{
			name: "only 1 option false",
			opts: []option.Option{
				option.UseMetaSessionCache(false),
			},
			expect: false, // current default in service.New
		},
		{
			name: "only 1 option true",
			opts: []option.Option{
				option.UseMetaSessionCache(true),
			},
			expect: true,
		},
		{
			name: "more options",
			opts: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithMax,
				option.UseMetaSessionCache(true),
			},
			expect: true,
		},
		{
			name: "more options with no UseMetaSessionCache",
			opts: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDWithMax,
			},
			expect: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// db can be nil for this test because New stores the reference and
			// we only verify the option wiring without establishing a session.
			svc, err := New(ctx, nil, "foo", tc.opts...)
			if err != nil {
				t.Fatalf("New() returned error: %v", err)
			}
			if svc.useMetaSessionCache != tc.expect {
				t.Fatalf("useMetaSessionCache mismatch: got %v, want %v", svc.useMetaSessionCache, tc.expect)
			}
		})
	}
}
