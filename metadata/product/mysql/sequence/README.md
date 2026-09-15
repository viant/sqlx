# MySQL sequencing

## Default: original transient transaction

`PresetIDWithTransientTransaction` is the default, preserving original Datly and
SQLX selection. `transient.go`, `handler.go` and `udf.go` remain byte-for-byte
unchanged from `24e180f`. The generic exact-value API invokes the original
NextSequence path and adapts its returned range without changing its SQL,
transaction, session, locking, cleanup or retry semantics.

No allocator table is needed. Source AUTO_INCREMENT is advanced by transient
source INSERTs in a separate transaction which SQLX rolls back. A caller's
transaction is not completed by this operation.

The mechanism needs a spare connection when a caller already holds a transaction,
and may wait on source locks held by that caller. Source defaults/triggers and
applicable constraints execute. Transactional rows roll back; nontransactional
or external effects need not. Original FK toggling/restoration, advisory lock
names, timeout/retries and the last-bound-argument identity assumption remain
unchanged. These limits are documented and tested, not repaired by this patch.
go-sql-driver/mysql can invalidate the caller connection/transaction when a
blocked allocation context is cancelled. SQLX does not commit or roll back that
caller transaction, but it cannot promise the driver-cancelled handle remains
usable. Cancellation tests therefore require an error, no committed source rows
and no silent allocator-table fallback; noncancelled caller ownership has a
separate positive test.

## Explicit `reservation` option

The native InnoDB table allocator is opt-in. Provision with
`Store.Install(ctx, db)` outside writer transactions, then pass
`dialect.PresetIDWithReservation` to the insert/sequence service. It uses
`sqlx_allocator.sequence_reservations`, caller-transaction row locking and no
transient source INSERTs. It can operate with a single caller connection.

Runtime credentials need SELECT/INSERT/UPDATE on native metadata and source
access. It honors native field mapping, increment/offset and integer bounds.
Rollback can release its reservation. Unmanaged source AUTO_INCREMENT inserts
do not see outstanding metadata reservations; do not mix generators. The default
never switches to this optional owner when the transient mechanism fails.
