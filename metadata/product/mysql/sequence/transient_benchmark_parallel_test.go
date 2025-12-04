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

// Parallel version of BenchmarkTransientHandle
func BenchmarkTransientHandleParallel(b *testing.B) {
	dsn := os.Getenv("MYSQL_BENCH_DSN")
	if dsn == "" {
		b.Skip("set MYSQL_BENCH_DSN to run this benchmark")
	}
	schema := os.Getenv("MYSQL_BENCH_SCHEMA")
	if schema == "" {
		b.Skip("set MYSQL_BENCH_SCHEMA to run this benchmark")
	}

	catalog := ""
	table := "t_seq_bench_par"
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

	// Ensure table exists and is clean
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
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var seq sink.Sequence
			if _, err := h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
				b.Fatalf("Handle failed: %v", err)
			}
		}
	})
}

// Parallel version of BenchmarkTransientHandleMulti (records > 1)
func BenchmarkTransientHandleMultiParallel(b *testing.B) {
	dsn := os.Getenv("MYSQL_BENCH_DSN")
	if dsn == "" {
		b.Skip("set MYSQL_BENCH_DSN to run this benchmark")
	}
	schema := os.Getenv("MYSQL_BENCH_SCHEMA")
	if schema == "" {
		b.Skip("set MYSQL_BENCH_SCHEMA to run this benchmark")
	}

	catalog := ""
	table := "t_seq_bench_par_multi"
	idCol := "id"

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
		target := seq.Value + (records-1)*inc
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
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var seq sink.Sequence
			if _, err := h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
				b.Fatalf("Handle failed: %v", err)
			}
		}
	})
}

// Parallel comparison: Handle vs HandleLegacy
func BenchmarkTransientHandleVsLegacyParallel(b *testing.B) {
	dsn := os.Getenv("MYSQL_BENCH_DSN")
	if dsn == "" {
		b.Skip("set MYSQL_BENCH_DSN to run this benchmark")
	}
	schema := os.Getenv("MYSQL_BENCH_SCHEMA")
	if schema == "" {
		b.Skip("set MYSQL_BENCH_SCHEMA to run this benchmark")
	}

	catalog := ""
	table := "t_seq_bench_par_cmp"
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

	b.Run("HandleParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var seq sink.Sequence
				if _, err := h.Handle(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
					b.Fatalf("Handle failed: %v", err)
				}
			}
		})
	})

	b.Run("HandleLegacyParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var seq sink.Sequence
				if _, err := h.HandleLegacy(ctx, db, &seq, option.Options(opts).Interfaces()...); err != nil {
					b.Fatalf("HandleLegacy failed: %v", err)
				}
			}
		})
	})

	/////////
	// Default to 10 record; override with env if needed
	recordsN := int64(10)
	if v := os.Getenv("MYSQL_BENCH_RECORDS_N"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			recordsN = n
		}
	}

	builderN := func(seq *sink.Sequence) (*sqlx.SQL, int64, error) {
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

	optsN := []option.Option{
		dialect.PresetIDWithTransientTransaction,
		option.RecordCount(recordsN),
		args,
		builderN,
		mysqlprod.MySQL5(),
	}

	b.Run("HandleParallel N-rec", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var seq sink.Sequence
				if _, err := h.Handle(ctx, db, &seq, option.Options(optsN).Interfaces()...); err != nil {
					b.Fatalf("Handle failed: %v", err)
				}
			}
		})
	})

	b.Run("HandleLegacyParallel N-rec", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var seq sink.Sequence
				if _, err := h.HandleLegacy(ctx, db, &seq, option.Options(optsN).Interfaces()...); err != nil {
					b.Fatalf("HandleLegacy failed: %v", err)
				}
			}
		})
	})

}
