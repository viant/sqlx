package read_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/read"
)

func BenchmarkReadRetrySQLite(b *testing.B) {
	for _, count := range []int{1, 100} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/retry=%t", count, enabled), func(b *testing.B) {
				h := sqlite.New(b, "CREATE TABLE records(id INTEGER)", fmt.Sprintf("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<%d) INSERT INTO records SELECT x FROM n", count))
				var opts []read.Option
				calls := 0
				if enabled {
					opts = append(opts, read.WithRetry(retryPolicy(h.DB, &calls)))
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					reader, err := read.New(ctx, h.DB, "SELECT id FROM records", func() any { return &lookupRow{} }, opts...)
					if err != nil {
						b.Fatal(err)
					}
					err = reader.QueryAll(ctx, func(any) error { return nil })
					if reader.Stmt() != nil {
						reader.Stmt().Close()
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
