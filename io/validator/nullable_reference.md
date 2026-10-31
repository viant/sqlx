Composite nullable reference checks
===================================

Schema-aware generators can retain a composite foreign key's physical member
columns in a `refKey` struct tag, alongside each member's ordinary `sqlx`
reference mapping. For example, both fields participating in a nullable
`(scope_id, parent_id)` FK carry `refKey:"scope_id,parent_id"`.

This metadata preserves SQL MATCH SIMPLE nullability: when any member of the
effective tuple is SQL NULL, scalar reference lookups for that tuple do not
require a parent. Required-field, uniqueness and unrelated reference checks
remain enabled. Zero and empty string values are not NULL. A group must retain
all member columns and a consistent reference target; malformed metadata fails
before querying.

Sparse candidates use matched, normalized Previous values for omitted members.
An omitted value is not assumed NULL without Previous or an explicit insert
candidate policy. A deferred member cannot establish a NULL tuple. Coverage is
per candidate, so a nullable row does not exempt another row's nonnull lookup.

Nonnull reference lookups keep their existing behavior. The database's complete
foreign-key constraint remains authoritative for validating tuple combinations
and enforcing atomic writes. This tag does not turn independent scalar checks
into a composite authorization or version-evidence proof.

SQLite discovery includes the pragma foreign-key ID in synthetic constraint
names, keeping independent FKs to the same table distinct during inference.
