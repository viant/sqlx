# Transaction-bound scoped sequences

`Reserve(ctx, tx, Request)` reserves exact numeric values inside the supplied
transaction for a physical table/column and typed scope tuple. It supports
numeric columns which are not identity/primary-key fields. It never writes
application rows, completes a transaction, or exposes a process-local counter.

```go
values, err := sequence.Reserve(ctx, tx, sequence.Request{
    Dialect: "sqlite", Table: "message", Column: "sequence",
    Scope: []sequence.Scope{{Column: "turn_id", Value: turnID}},
    Count: 2, Supplied: []int64{3},
})
```

With stored turn maximum 2, this returns 4 and 5. Other turns have independent
counters. Supplied values are skipped, not rewritten. Pass the complete batch's
supplied values on every reservation; count zero performs no allocation.

The ledger `sqlx_scoped_sequences` belongs to the same schema as the source.
SQLite can create it within its transaction and takes write intent before reads.
MySQL uses an InnoDB counter row lock; call `Provision(ctx, db, "mysql")` outside
business transactions first, because MySQL DDL implicitly commits. Qualified
source tables require the ledger in that schema. Unsupported dialects fail.

Counters advance in the caller transaction, so rollback restores reservations.
Keep the transaction open through the corresponding source writes. Scope values
are bound parameters; identifiers are parsed/quoted. Scope fields must be
non-null strings, booleans or integers. Use stable field types for one partition.
`Collision` proves a generated tuple/value collision while rejecting a present
primary identity; it is intended for framework-controlled rollback/replay.

Live MySQL tests use `SQLX_SCOPED_MYSQL_DSN`, which must name an isolated test
schema: they provision the ledger and create/drop fixture tables. SQLite tests
and live MySQL tests include genuinely independent database connections.
