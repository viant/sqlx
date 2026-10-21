# Transaction-Local Statement Reuse

Ordinary `Service.Exec` is unchanged. Callers that already own a transaction can
opt into bounded UPDATE statement reuse:

```go
err := update.WithStatementRunner(tx, func(run *update.StatementRunner) error {
    for _, row := range rows {
        if _, err := run.Exec(ctx, updater, row); err != nil {
            return err
        }
    }
    return nil
})
// Commit only on success; the transaction remains caller-owned.
```

The runner retains only the last exact SQL shape, and only for the same service,
mapping generation, row type, standard builder and presence-marker mapping.
SQL construction, value binding, predicates, hooks, cancellation and affected-row
checks still run for every call. Custom builders and nontransactional dialects
do not retain statements. Services and their configuration must not be mutated
concurrently, just as with ordinary execution.

Call `run.Reset()` before and after non-UPDATE SQL, DDL, or authored queue barriers.
Do not put the runner in application-visible providers or contexts. Concurrent
or reentrant calls return `ErrRunnerBusy`; after the callback returns, escaped
calls return `ErrRunnerClosed`. The runner has no public Close method.

Finalization seals admission, joins accepted work and closes the statement before
returning. Callback, execution, and cleanup errors remain discoverable through
`errors.Is`/`errors.As`, including errors ignored by the callback. Panics propagate
after cleanup; when cleanup also fails, `RunnerPanicError` retains both the
original panic value and error causes. The runner never commits or rolls back.

Driver panics are rethrown after runner cleanup, but cannot be made transaction-
safe by this API: `database/sql` may retain an internal transaction lease when a
driver panics. Drivers must return errors rather than panic. Hook/callback panics
do not have this standard-library limitation.
