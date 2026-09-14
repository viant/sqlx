package aerospike

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/viant/sqlx/io/read/cache"
)

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
