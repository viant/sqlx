package cache

import (
	"reflect"
	"testing"
)

func TestProjectionCanonicalMetadata(t *testing.T) {
	stored := []ProjectionField{{DimensionKey: "campaign.id"}, {MeasureKey: "metrics.spend"}}
	for _, tc := range []struct {
		name      string
		requested []ProjectionField
		want      []int
		match     bool
	}{
		{"canonical_reordered", []ProjectionField{{MeasureKey: "metrics.spend"}, {DimensionKey: "campaign.id"}}, []int{1, 0}, true},
		{"missing_dimension", []ProjectionField{{MeasureKey: "metrics.spend"}}, nil, false},
		{"unknown_measure", []ProjectionField{{DimensionKey: "campaign.id"}, {MeasureKey: "metrics.other"}}, nil, false},
		{"dimension_only", []ProjectionField{{DimensionKey: "campaign.id"}}, []int{0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexes, ok, _, err := (Projection{Stored: stored}).Indexes(tc.requested)
			if err != nil || ok != tc.match || !reflect.DeepEqual(indexes, tc.want) {
				t.Fatalf("projection=%v,%v,%v", indexes, ok, err)
			}
		})
	}
}
