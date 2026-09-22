package aerospike

import as "github.com/aerospike/aerospike-client-go"

// Client returns the borrowed client used by this cache. Its creator (or Pool)
// owns its lifetime. Callers may use it for related cache control records, but
// must not close it independently of the owning cache pool.
func (c *Cache) Client() *as.Client { return c.client }
