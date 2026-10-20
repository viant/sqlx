# Native schema artifacts

`EnsureTable` remains a non-destructive, existence-based provisioning helper.
`Extract`, `Apply`, and `ApplySQL` handle explicit same-dialect schema artifacts.
They do not infer migrations or drop existing objects.

```go
artifact, err := schema.Extract(ctx, sourceDB, schema.ExtractOptions{
    Tables: []string{"orders"}, // decoded literal names, not SQL paths
    IncludeDependencies: true,
})
if err != nil { return err }
result, err := schema.Apply(ctx, targetDB, artifact, schema.ApplyOptions{Atomic: true})
```

`Tables` is required. Constraints and indexes are in the default table scope;
views and triggers are opt-in. `IncludeDependencies` includes referenced tables;
otherwise an unselected foreign-key target is an error. Artifacts contain native
ordered statements, object records, dialect/driver/schema, omissions, and SHA-256
of `artifact.SQL()`. Apply verifies a supplied checksum before executing SQL.
An incomplete artifact requires explicit `AllowPartial` during extraction and
again during application. Query failures remain errors with `AllowPartial`.
No data rows are extracted. `IncludeSequenceState` separately requests current
counters; identity/automatic-ID definitions are preserved by default.

## Implemented extraction scope

| Dialect | Implementation and verification | Explicit limits |
| --- | --- | --- |
| SQLite | Stored `sqlite_schema` table/index/view/trigger definitions. Executable roundtrip tests cover generated/default values, identities, numeric precision, ordered composite keys, foreign keys, indexes, quoted names, view/trigger behavior, and counters. | Main schema only. Virtual tables/extensions are omitted with an error. Views include all source views; view/trigger dependencies conservatively require all user tables. IncludeDependencies permits that expansion. Missing view/trigger dependencies are checked by preparing EXPLAIN programs. |
| PostgreSQL 14+ | Native catalog types, expressions, constraints and indexes; ordinary heap tables; owned serial/identity sequences and options; FK closure with a separate ALTER phase, including cycles. Live PostgreSQL 16 tests replay definitions and counters and exercise defaults and constraints. | Inheritance, partitioning, row security, extensions/custom types, explicit collations, custom storage/compression/tablespaces/replica identity, shared/unowned sequences, and view/trigger extraction are unsupported. Foreign keys and owned sequences must stay in the selected schema. Extraction uses a read-only transaction and a local pg_catalog search_path. |
| MySQL | Native SHOW CREATE TABLE plus foreign-key dependency discovery. Live MySQL 8 tests replay definitions and counters and exercise defaults and constraints. | Cyclic foreign keys, cross-schema dependencies, views and triggers are unsupported. Current AUTO_INCREMENT table state is removed unless requested. |
| BigQuery | Native INFORMATION_SCHEMA.TABLES.ddl preserves nested STRUCT/ARRAY definitions and native table options. Query-contract tests preserve returned DDL exactly. | Always incomplete until index/FK dependency closure is certified; AllowPartial is required. Only native base-table DDL is exported. No live BigQuery certification. |

Native SQL preserves its namespace semantics. PostgreSQL target schemas and
BigQuery datasets must already exist. There is no schema remapping or cross-vendor
conversion. Native statements are authoritative; the optional nested `Field`
descriptor does not flatten BigQuery fields or reconstruct DDL from flat columns.
The extractor expects a quiescent source. Grants, owners, routines, extensions,
application functions, comments, and full-database backup/recreation are outside
its requested table scope.
PostgreSQL checks the connected server's `server_version_num` before reading its
14+ catalogs; a registry dialect's capability baseline is not its server version.

## Execution

`ApplySQL` parses the complete script before execution. The script reader supports
quoted strings/identifiers, comments, PostgreSQL dollar-quoted bodies and SQLite
trigger blocks. Client DELIMITER/GO/backslash/dot commands, standalone executable
MySQL comments, COPY streaming, lexical-setting changes and unquoted procedural
blocks are rejected with a line number. PostgreSQL lexical rules assume standard_conforming_strings=on;
use E strings for escaped quotes. MySQL scripts assume default string-escape
rules. Sessions must use these lexical settings.
Use one complete SQL statement per `Artifact.Statements` entry.

All statements use one pinned connection. `Atomic` uses an owned DDL transaction
on SQLite/PostgreSQL. A supplied `Tx` is caller-owned and is never committed or
rolled back; it cannot be combined with Atomic. MySQL/BigQuery transactional DDL
requests are rejected. PostgreSQL sequence state and authored function effects
retain their server-defined nontransactional behavior. On failure, Executed
counts successful Exec calls, including any later rolled back. RolledBack reports
an owned rollback; it makes no claim that a caller transaction has committed.

The optional live tests create and drop two uniquely named databases using an
explicit admin DSN. Run only against an isolated server:

```sh
SQLX_SCHEMA_POSTGRES_DSN='postgres://user:password@localhost:5432/postgres?sslmode=disable' GOWORK=off go test ./io/schema -run TestLivePostgreSQL
SQLX_SCHEMA_MYSQL_DSN='user:password@tcp(localhost:3306)/mysql' GOWORK=off go test ./io/schema -run TestLiveMySQL
```
