package sequence_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info/dialect"
	mysqlprod "github.com/viant/sqlx/metadata/product/mysql"
	seqpkg "github.com/viant/sqlx/metadata/product/mysql/sequence"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

//actualDoNext, err := testCase.handler01.Handle(
//	 ctx,
//	 db,
//	 testCase.sink,
//&testCase.args,
//testCase.tx,
//testCase.insertStrategy,
//option.RecordCount(testCase.recordCnt)
// )

// Benchmark handle reserving IDs via transient inserts.
// Required env variables:
//   - MYSQL_BENCH_DSN:     DSN for MySQL (e.g. "user:pass@tcp(127.0.0.1:3306)/")
//   - MYSQL_BENCH_SCHEMA:  Schema/DB name (e.g. "testdb")
//
// Optional env variables:
//   - MYSQL_BENCH_CATALOG: Catalog/DB name (often empty for MySQL)
//   - MYSQL_BENCH_RECORDS: Record count to reserve per call (default: 1)
//
// Example run:
//
//	MYSQL_BENCH_DSN='user:pass@tcp(localhost:3306)/' \
//	MYSQL_BENCH_SCHEMA='testdb' \
//	go test -bench=BenchmarkTransientHandle -run=^$ ./metadata/product/mysql/sequence -benchmem
func BenchmarkTransientHandle(b *testing.B) {
	dsn := os.Getenv("MYSQL_BENCH_DSN")
	if dsn == "" {
		b.Skip("set MYSQL_BENCH_DSN to run this benchmark")
	}
	schema := os.Getenv("MYSQL_BENCH_SCHEMA")
	if schema == "" {
		b.Skip("set MYSQL_BENCH_SCHEMA to run this benchmark")
	}

	catalog := ""

	// Auto-manage a simple benchmark table with an AUTO_INCREMENT PK.
	table := "t_seq_bench"
	idCol := "id"
	records := int64(1)
	if v := os.Getenv("MYSQL_BENCH_RECORDS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			records = n
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("open dsn: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		b.Fatalf("ping: %v", err)
	}

	// Ensure benchmark table exists and is clean. We drop and recreate to isolate runs.
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`"); err != nil {
		b.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE `" + schema + "`.`" + table + "` ( `" + idCol + "` BIGINT AUTO_INCREMENT PRIMARY KEY, `foo_name` TEXT )"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	b.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`")
	})

	// Minimal SequenceSQLBuilder for the target table.
	// This builder is called twice per Handle call:
	//  - first time: we ignore the id argument (Handle sets it to NULL)
	//  - second time: we compute the explicit id to reserve based on base and records
	builder := func(seq *sink.Sequence) (*sqlx.SQL, int64, error) {
		// Compute the last id to insert in the reserved block based on current base.
		// After the first transient insert, Handle sets seq.Value = base.
		inc := seq.IncrementBy
		if inc == 0 {
			inc = 1
		}
		target := seq.Value
		if records > 1 {
			target = seq.Value + (records-1)*inc
		}
		// The id argument is passed by pointer to allow Handle to override it with NULL for the first insert.
		var idArg int64 = target
		var name = fmt.Sprintf("%d", idArg)
		sql := &sqlx.SQL{
			Query: "INSERT INTO `" + schema + "`.`" + table + "` (foo_name,`" + idCol + "`) VALUES (?,?)",
			Args:  []interface{}{&name, &idArg},
		}
		return sql, records, nil
	}

	args := option.NewArgs(catalog, schema, table)
	opts := []option.Option{
		dialect.PresetIDWithTransientTransaction,
		option.RecordCount(records),
		args,
		builder,
		mysqlprod.MySQL5(),
	}

	ctx := context.Background()
	h := &seqpkg.Transient{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var seq sink.Sequence
		// Each iteration reserves IDs transiently (the handler rolls back internally).
		if _, err := h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
			b.Fatalf("Handle failed: %v", err)
		}
	}
}

// BenchmarkTransientHandleMulti benchmarks reserving multiple IDs (record count > 1)
// using the same DB/table setup as BenchmarkTransientHandle. To override the
// number of records, set MYSQL_BENCH_RECORDS to a value > 1; otherwise it defaults to 2.
func BenchmarkTransientHandleMulti(b *testing.B) {
	dsn := os.Getenv("MYSQL_BENCH_DSN")
	if dsn == "" {
		b.Skip("set MYSQL_BENCH_DSN to run this benchmark")
	}
	schema := os.Getenv("MYSQL_BENCH_SCHEMA")
	if schema == "" {
		b.Skip("set MYSQL_BENCH_SCHEMA to run this benchmark")
	}

	catalog := ""
	table := "t_seq_bench_multi"
	idCol := "id"

	// Default to 2 if env var is not set or <= 1
	records := int64(2)
	if v := os.Getenv("MYSQL_BENCH_RECORDS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 1 {
			records = n
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("open dsn: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		b.Fatalf("ping: %v", err)
	}

	// Ensure benchmark table exists and is clean.
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`"); err != nil {
		b.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE `" + schema + "`.`" + table + "` ( `" + idCol + "` BIGINT AUTO_INCREMENT PRIMARY KEY, `foo_name` TEXT )"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	b.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS `" + schema + "`.`" + table + "`")
	})

	builder := func(seq *sink.Sequence) (*sqlx.SQL, int64, error) {
		inc := seq.IncrementBy
		if inc == 0 {
			inc = 1
		}
		target := seq.Value
		if records > 1 {
			target = seq.Value + (records-1)*inc
		}
		var idArg int64 = target
		var name = fmt.Sprintf("%d", idArg)
		sql := &sqlx.SQL{
			Query: "INSERT INTO `" + schema + "`.`" + table + "` (foo_name,`" + idCol + "`) VALUES (?,?)",
			Args:  []interface{}{&name, &idArg},
		}
		return sql, records, nil
	}

	args := option.NewArgs(catalog, schema, table)
	opts := []option.Option{
		dialect.PresetIDWithTransientTransaction,
		option.RecordCount(records),
		args,
		builder,
		mysqlprod.MySQL5(),
	}

	ctx := context.Background()
	h := &seqpkg.Transient{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var seq sink.Sequence
		if _, err := h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
			b.Fatalf("Handle failed: %v", err)
		}
	}
}
