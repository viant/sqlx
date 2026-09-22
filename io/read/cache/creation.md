# Cache creation observations

Both native AFS and Aerospike caches expose:

```go
service.SetCreationObserver(func(kind string, entries int) {
    // kind is "lazy" or "warmup".
    // Export entries to the corresponding application counter.
})
```

Observers run after a successful publication. Counts include logical query/group
entries and index markers, excluding overflow chunks. A refresh/replacement is a
new publication. Hits, reused AFS warmups, rollbacks, and failed publications do
not increment these counts. Empty results count when their entry/marker publishes.
Callbacks must tolerate concurrent calls.

`cache.Meta.CreatedTimeMs` persists the entry's creation timestamp in Unix
milliseconds. `cache.Stats.CreatedTime` exposes it as a `*time.Time`, alongside
`ExpiryTime`. Creation time remains unchanged on hits; refresh creates a new
publication with a new timestamp. Legacy entries without a creation timestamp
return a nil `CreatedTime` instead of inventing a value. AFS preserves stored
expiry timestamps when reading metadata rather than losing them from statistics.
Aerospike timestamps are stored on query roots and warmup publication markers.

Aerospike forced refresh retires both the exact-query and warmup candidates so
an old warmup cannot satisfy the forced database read. It is a query-publication
operation; Datly's shared generation controls provide component/view-wide
invalidation and fence older in-flight writers.

Validation includes native SQLite/AFS tests and opt-in live Aerospike tests:

```sh
DATLY_TEST_AEROSPIKE=aerospike://127.0.0.1:3000/test \
  go test -race ./io/read/cache/... ./io/read -count=1
```

Use a dedicated test Aerospike namespace. The live tests write unique, short-lived
cache keys and do not truncate shared sets or namespaces.
