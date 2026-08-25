package sequence_test

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info/dialect"
	mysqlprod "github.com/viant/sqlx/metadata/product/mysql"
	seqpkg "github.com/viant/sqlx/metadata/product/mysql/sequence"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Race condition tests for Handle. These tests require a real MySQL
// and will be skipped unless TEST_MYSQL_DSN and TEST_MYSQL_SCHEMA are set.
// A temporary table with AUTO_INCREMENT PK is created and dropped automatically.

//Run with race detector:
//- go test -race ./metadata/product/mysql/sequence -run TestHandleRace

//- Or run the whole package with -race to see all.

//- Run only single-record: go test -race  ./metadata/product/mysql/sequence -run '^TestHandleRace_SingleRecord$'
//- Run only multi-record: go test -race  ./metadata/product/mysql/sequence '^TestHandleRace_MultiRecord$'
//- Run both: go test -race  ./metadata/product/mysql/sequence 'TestHandleRace'

func newTestDB(t *testing.T) (*sql.DB, string, string, string, string, int64) {
	t.Helper()

	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN and TEST_MYSQL_SCHEMA to run race tests")
	}
	schema := os.Getenv("TEST_MYSQL_SCHEMA")
	catalog := ""
	if schema == "" {
		t.Skip("set TEST_MYSQL_SCHEMA to run race tests")
	}
	records := int64(1)
	if v := os.Getenv("MYSQL_TEST_RECORDS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			records = n
		}
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open dsn: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Create a temporary table for this test and ensure cleanup.
	table := "t_seq_race_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	idCol := "id"
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE `" + schema + "`.`" + table + "` ( `" + idCol + "` BIGINT AUTO_INCREMENT PRIMARY KEY )"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`")
	})

	return db, catalog, schema, table, idCol, records
}

func builderFor(schema, table, idCol string, records int64) func(*sink.Sequence) (*sqlx.SQL, int64, error) {
	return func(seq *sink.Sequence) (*sqlx.SQL, int64, error) {
		inc := seq.IncrementBy
		if inc == 0 {
			inc = 1
		}
		target := seq.Value
		if records > 1 {
			target = seq.Value + (records-1)*inc
		}
		var idArg int64 = target
		sql := &sqlx.SQL{
			Query: "INSERT INTO `" + schema + "`.`" + table + "` (`" + idCol + "`) VALUES (?)",
			Args:  []interface{}{&idArg},
		}
		return sql, records, nil
	}
}

func runRace(t *testing.T, workers, iterations int, records int64) {
	db, catalog, schema, table, idCol, _ := newTestDB(t)
	defer db.Close()

	args := option.NewArgs(catalog, schema, table)
	builder := builderFor(schema, table, idCol, records)
	opts := []option.Option{
		dialect.PresetIDWithTransientTransaction,
		option.RecordCount(records),
		args,
		builder,
		mysqlprod.MySQL5(),
	}

	ctx := context.Background()
	h := &seqpkg.Transient{}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				var seq sink.Sequence
				var err error
				_, err = h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...)
				if err != nil {
					t.Errorf("handler failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestHandleRace_SingleRecord(t *testing.T) {
	t.Parallel()
	runRace(t, 20, 20, 1)
}

func TestHandleRace_MultiRecord(t *testing.T) {
	t.Parallel()
	runRace(t, 20, 20, 2)
}
