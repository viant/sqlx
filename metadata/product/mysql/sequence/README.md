# MySQL native reservations

MySQL's default numeric preset strategy uses `ReserveSequence` and an InnoDB
counter in `sqlx_allocator.sequence_reservations`. `Store.Install(ctx, db)` must
run once, outside writer transactions, using deployment credentials. Runtime
credentials need SELECT/INSERT/UPDATE on that table and normal source-table
read/write permissions. Allocation never runs DDL or opens a second transaction
when supplied a caller transaction.

The source column must be an integer AUTO_INCREMENT column in an InnoDB table.
The native mapper selects it; `SequenceField` can select a Go field explicitly
when the mapping contains more than one identity. Session increment/offset and
signed/unsigned column limits are respected, within the int64 value contract.

First use and later calls lock the same metadata row. A current `FOR UPDATE`
read observes the largest source ID even under REPEATABLE READ. The counter is
then advanced and verified under the same transaction. No transient source row,
source ALTER, FK toggle, application trigger or second-connection lock is used.
Caller completion is never taken over. Standalone insertion shares its own
native insert transaction with allocation, including one-connection pools.

This is a transactional SQLX allocator. Rollback can release a reservation if
normal entity insertion has not already advanced the source AUTO_INCREMENT
counter. SQLX default mapped-ID inserts and Datly allocation use the same owner.
Raw SQL or inserts omitting the mapped identity bypass the owner; source-table
AUTO_INCREMENT cannot see outstanding SQLX reservations without source DML or
DDL, both forbidden during reservation. Such unmanaged generators must not be
mixed with outstanding preallocated IDs. Ordinary explicit supplied IDs still
remain subject to database uniqueness checks.

The old transient strategy remains explicitly selectable for callers that
understand its source INSERT/trigger and transaction limitations. It is no longer
the default and is not used by Datly's sequencer.
