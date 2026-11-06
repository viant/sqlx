# ClickHouse support

Import this package to register the ClickHouse `database/sql` driver and SQLX
product metadata:

```go
import _ "github.com/viant/sqlx/metadata/product/clickhouse"

db, err := sql.Open("clickhouse", "clickhouse://user:password@host:9000/database")
```

The product supports schema, table, column, session, and version discovery;
parameterized reads; and batched inserts. ClickHouse prepared statements are
insert batches, so SQLX runs reads directly and commits insert batches before
returning. ClickHouse does not provide general transactions, upserts, or
autoincrement IDs through this product.

Set `SQLX_CLICKHOUSE_DSN` to run the live integration test:

```bash
SQLX_CLICKHOUSE_DSN='clickhouse://user:password@host:9000/database' \
  go test ./metadata/product/clickhouse -run TestClickHouseIntegration -count=1
```
