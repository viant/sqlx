package aerospike

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/viant/sqlx/io/read/cache"
)

func TestMismatchFieldNamesFollowProjection(t *testing.T) {
	entry := &cache.Entry{Meta: cache.Meta{
		Fields: []*cache.Field{
			{ColumnName: "DOOH_BUDGET"},
			{ColumnName: "PRIMARY_START_DATE"},
		},
		StoredFields: []cache.ProjectionField{
			{Name: "DOOH_BUDGET"},
			{Name: "PRIMARY_START_DATE"},
		},
		ProjectedIndexes: []int{1, 0},
	}}
	for _, test := range []struct {
		index int
		want  string
	}{
		{0, "PRIMARY_START_DATE"}, {1, "DOOH_BUDGET"}, {-1, ""}, {2, ""},
	} {
		mismatch := &cache.TypeMismatch{Index: test.index}
		if got := mismatchFieldName(entry, mismatch); got != test.want {
			t.Errorf("field at %d = %q, want %q", test.index, got, test.want)
		}
		if got := mismatchStoredFieldName(entry, mismatch); got != test.want {
			t.Errorf("stored field at %d = %q, want %q", test.index, got, test.want)
		}
	}
	entry.Meta.ProjectedIndexes = nil
	if got := mismatchFieldName(entry, &cache.TypeMismatch{Index: 0}); got != "DOOH_BUDGET" {
		t.Fatalf("unprojected field = %q", got)
	}
	if mismatchFieldName(nil, nil) != "" || mismatchStoredFieldName(nil, nil) != "" {
		t.Fatal("absent mismatch must have no field name")
	}
}

func TestSourcesKeepIndependentScanTypes(t *testing.T) {
	service := &Cache{}
	var workers sync.WaitGroup
	failures := make(chan error, 32)
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			entry := &cache.Entry{}
			var id int
			var name string
			values := []interface{}{&id}
			entry.Meta.Type = []string{"int"}
			entry.Data = []byte(`[7]`)
			if index%2 != 0 {
				values = []interface{}{&name, &id}
				entry.Meta.Type = []string{"string", "int"}
				entry.Data = []byte(`["seven",7]`)
			}
			source := &Source{cache: service, entry: entry}
			if ok, err := source.CheckType(context.Background(), values); err != nil || !ok {
				failures <- fmt.Errorf("source %d type check: %v, %v", index, ok, err)
				return
			}
			if err := source.Scanner(context.Background())(values...); err != nil {
				failures <- err
				return
			}
			if id != 7 || index%2 != 0 && name != "seven" {
				failures <- fmt.Errorf("source %d decoded %d, %q", index, id, name)
			}
		}(i)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}
