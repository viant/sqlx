package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	mysqlprod "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/option"
)

// Entity used for inserts (auto-increment ID)
type insertRaceEntity struct {
	ID   int64  `sqlx:"name=foo_id,generator=autoincrement"`
	Name string `sqlx:"foo_name"`
	Bar  int    `sqlx:"bar"`
}

// TestInsertRace_CachedSessions_MySQL validates that multiple inserters using cached
// sessions (and different batch sizes) do not reserve/assign overlapping IDs when
// inserting concurrently into the same table.
func TestInsertRace_CachedSessions_MySQL(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN to run this test")
	}

	//os.Setenv("DEBUG_SEQUENCER", "true")

	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	// Phase 1 (global warm-up): detect product/dialect once in a single goroutine
	// so the registry and service-level caches are primed before any parallel work.
	// Passing the MySQL product option here avoids any product probing on the hot path.
	_, err = config.Dialect(ctx, db, mysqlprod.MySQL5())
	require.NoError(t, err)

	table := "t_insert_race"
	// Prepare table (single auto-increment PK + two payload columns)
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf("CREATE TABLE %s (foo_id BIGINT AUTO_INCREMENT PRIMARY KEY, foo_name TEXT, bar INT)", table),
	}
	for _, s := range stmts {
		_, err = db.ExecContext(ctx, s)
		require.NoError(t, err, s)
	}

	// We will create multiple inserter Services per batch size to ensure cached
	// sessions are used within each Service instance.
	//batchSizes := []int{1, 2, 3}
	batchSizes := []int{1, 2, 3}
	servicesPerBatch := 5
	iterationsPerService := 15

	// Each Exec call will insert exactly bs records to keep expected totals simple.
	makeRecords := func(n int, base string) []*insertRaceEntity {
		out := make([]*insertRaceEntity, n)
		for i := 0; i < n; i++ {
			out[i] = &insertRaceEntity{ID: 0, Name: fmt.Sprintf("%s-%d", base, i), Bar: i}
		}
		return out
	}

	totalExpected := 0
	var inserters []*insert.Service
	var svcBatchSizes []int // aligned with inserters slice to track each service batch size
	for _, bs := range batchSizes {
		for i := 0; i < servicesPerBatch; i++ {
			svc, err := insert.New(ctx, db, table /* service-level options are metadata only */, option.BatchSize(bs))
			require.NoError(t, err)
			inserters = append(inserters, svc)
			svcBatchSizes = append(svcBatchSizes, bs)
			totalExpected += iterationsPerService * bs
		}
	}

	// Phase 1 (per-service warm-up): create the first session for each service sequentially
	// to prevent concurrent product/dialect detection inside NewSession(). This ensures
	// every service has a cached session before any parallel Exec calls begin.
	for i, svc := range inserters {
		bs := svcBatchSizes[i]
		_, err := svc.NewSession(ctx, &insertRaceEntity{}, db, bs)
		require.NoError(t, err)
	}

	// Run all services concurrently; each service uses its own cached session with
	// a fixed batch size, and we pass the same batch size into Exec to ensure the
	// cached session is reused on subsequent calls.
	var wg sync.WaitGroup
	errs := make(chan error, len(inserters)*iterationsPerService)

	idx := 0
	for _, bs := range batchSizes {
		for i := 0; i < servicesPerBatch; i++ {
			svc := inserters[idx]
			idx++
			wg.Add(1)
			// Launch worker only after per-service warm-up above to avoid any first-use races.
			go func(bs int, s *insert.Service, id int) {
				defer wg.Done()
				// Phase 1 (hot-path option): include MySQL product and transient strategy
				// with every Exec so metadata handlers never attempt product detection.
				opts := []option.Option{
					option.BatchSize(bs),
					dialect.PresetIDWithTransientTransaction,
					mysqlprod.MySQL5(),
				}
				for it := 0; it < iterationsPerService; it++ {
					records := makeRecords(bs, fmt.Sprintf("svc-%d-iter-%d", id, it))
					if _, _, err := s.Exec(ctx, records, opts...); err != nil {
						errs <- err
						return
					}
				}
			}(bs, svc, idx)
		}
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	// Validate row count matches the expected number of records
	// and that all IDs are unique.
	var gotCount, distinctCount int
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&gotCount))
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(DISTINCT foo_id) FROM %s", table)).Scan(&distinctCount))
	require.Equal(t, totalExpected, gotCount, "unexpected row count - potential ID reservation conflict")
	require.Equal(t, totalExpected, distinctCount, "duplicate IDs detected")
}
