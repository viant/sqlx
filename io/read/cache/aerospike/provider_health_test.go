package aerospike

import (
	"context"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/viant/sqlx/io/read/cache"
	"testing"
	"time"
)

func TestExpectedMissDoesNotTripConfiguredCache(t *testing.T) {
	delay := time.Hour
	for _, tc := range []struct {
		name    string
		codes   []types.ResultCode
		blocked bool
	}{
		{"cold misses", []types.ResultCode{types.KEY_NOT_FOUND_ERROR, types.KEY_NOT_FOUND_ERROR, types.KEY_NOT_FOUND_ERROR}, false},
		{"miss interrupts failures", []types.ResultCode{types.TIMEOUT, types.KEY_NOT_FOUND_ERROR, types.TIMEOUT}, false},
		{"success interrupts failures", []types.ResultCode{types.TIMEOUT, types.OK, types.TIMEOUT}, false},
		{"real consecutive failures", []types.ResultCode{types.TIMEOUT, types.TIMEOUT}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := NewFailureHandler(1, &delay)
			defer failure.Close()
			cache := &Cache{failureHandler: failure}
			for _, code := range tc.codes {
				cache.handleResponseFailure(code)
			}
			if got := failure.IsProbing(); got != tc.blocked {
				t.Fatalf("probing=%v want=%v", got, tc.blocked)
			}
		})
	}
}

func TestReadFailureCountOwnedByRecordOperation(t *testing.T) {
	delay := time.Hour
	failure := NewFailureHandler(3, &delay)
	defer failure.Close()
	service := &Cache{namespace: "test", set: "health", failureHandler: failure, getRecordFn: func(*as.Key, ...string) (*as.Record, error) { return nil, types.NewAerospikeError(types.TIMEOUT) }}
	key, err := service.key("probe")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = service.getRecord(key)
	if failure.counter != 1 {
		t.Fatalf("one record failure counted %d times", failure.counter)
	}
	for i := 0; i < 2; i++ {
		_, _ = service.Get(context.Background(), "SELECT id FROM records", nil, &cache.Stats{})
	}
	if failure.counter != 3 || failure.IsProbing() {
		t.Fatalf("three record failures: count=%d probing=%v", failure.counter, failure.IsProbing())
	}
	_, _ = service.Get(context.Background(), "SELECT id FROM records", nil, &cache.Stats{})
	if failure.counter != 4 || !failure.IsProbing() {
		t.Fatalf("four record failures: count=%d probing=%v", failure.counter, failure.IsProbing())
	}
}
