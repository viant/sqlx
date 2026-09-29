// Package sequence owns transaction-bound scoped numeric allocation.
package sequence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/viant/sqlparser"
)

const LedgerTable = "sqlx_scoped_sequences"

// Scope binds physical columns to values. Values are always SQL parameters.
type Scope struct {
	Column string
	Value  any
}

// Request selects one independent numeric sequence. Supplied values from the
// complete invocation must be passed before allocating any generated values.
type Request struct {
	Dialect, Table, Column string
	Scope                  []Scope
	Count                  int
	Supplied               []int64
}

// Provision creates the counter table. MySQL callers provision before starting
// business transactions: DDL must never implicitly commit a caller transaction.
func Provision(ctx context.Context, db *sql.DB, dialect string) error {
	var ddl string
	switch strings.ToLower(dialect) {
	case "sqlite", "sqlite3":
		ddl = "CREATE TABLE IF NOT EXISTS " + LedgerTable + " (scope_key TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value>=0))"
	case "mysql":
		ddl = "CREATE TABLE IF NOT EXISTS " + LedgerTable + " (scope_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,value BIGINT NOT NULL) ENGINE=InnoDB"
	default:
		return fmt.Errorf("scoped sequences unsupported for dialect %q", dialect)
	}
	_, err := db.ExecContext(ctx, ddl)
	return err
}

// Reserve locks and advances one scope in the supplied transaction. It never
// commits/rolls back it, executes source INSERTs, or mutates application records.
// SQLite's first statement is a write, before any MAX/snapshot read.
func Reserve(ctx context.Context, tx *sql.Tx, r Request) ([]int64, error) {
	if tx == nil {
		return nil, fmt.Errorf("scoped sequence requires a transaction")
	}
	if r.Count < 0 || len(r.Scope) == 0 {
		return nil, fmt.Errorf("scoped sequence requires scope and nonnegative count")
	}
	dialect := strings.ToLower(r.Dialect)
	if dialect != "sqlite" && dialect != "sqlite3" && dialect != "mysql" {
		return nil, fmt.Errorf("scoped sequences unsupported for dialect %q", r.Dialect)
	}
	quote := func(value string) string {
		if dialect == "mysql" {
			return "`" + strings.ReplaceAll(value, "`", "``") + "`"
		}
		return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
	}
	parts, err := sqlparser.TableIdentifierParts(r.Table)
	if err != nil || len(parts) == 0 || len(parts) > 2 {
		return nil, fmt.Errorf("invalid scoped sequence table %q", r.Table)
	}
	tableParts := make([]string, len(parts))
	for i, part := range parts {
		tableParts[i] = quote(part)
	}
	tableSQL := strings.Join(tableParts, ".")
	ledger := quote(LedgerTable)
	if len(parts) == 2 {
		ledger = quote(parts[0]) + "." + ledger
	}
	column, err := singleIdentifier(r.Column)
	if err != nil {
		return nil, err
	}
	where := []string{}
	args := []any{}
	seen := map[string]bool{}
	canonical := []any{strings.Join(parts, "."), strings.ToLower(column)}
	scope := append([]Scope(nil), r.Scope...)
	sort.Slice(scope, func(i, j int) bool { return strings.ToLower(scope[i].Column) < strings.ToLower(scope[j].Column) })
	for _, item := range scope {
		name, err := singleIdentifier(item.Column)
		if err != nil {
			return nil, err
		}
		if item.Value == nil || seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("scope must contain distinct nonnull columns")
		}
		seen[strings.ToLower(name)] = true
		switch item.Value.(type) {
		case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		default:
			return nil, fmt.Errorf("scope %s must use a string, boolean or integer value", name)
		}
		where = append(where, quote(name)+" = ?")
		args = append(args, item.Value)
		canonical = append(canonical, strings.ToLower(name), item.Value)
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%x", sha256.Sum256(encoded))
	if dialect == "mysql" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO "+ledger+" (scope_key,value) VALUES (?,0) ON DUPLICATE KEY UPDATE scope_key=scope_key", key); err != nil {
			return nil, fmt.Errorf("lock scoped sequence (provision %s before use): %w", LedgerTable, err)
		}
	} else {
		// CREATE's non-no-op path acquires write intent; the ordinary path UPDATE
		// does so before any read, avoiding deferred-transaction upgrade races.
		if _, err = tx.ExecContext(ctx, "UPDATE "+ledger+" SET value=value WHERE 0"); err != nil {
			if _, createErr := tx.ExecContext(ctx, "CREATE TABLE "+ledger+" (scope_key TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value>=0))"); createErr != nil {
				if _, err = tx.ExecContext(ctx, "UPDATE "+ledger+" SET value=value WHERE 0"); err != nil {
					return nil, fmt.Errorf("lock scoped sequence: %w", createErr)
				}
			}
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO "+ledger+" (scope_key,value) VALUES (?,0) ON CONFLICT(scope_key) DO NOTHING", key); err != nil {
			return nil, err
		}
	}
	query := "SELECT value FROM " + ledger + " WHERE scope_key=?"
	if dialect == "mysql" {
		query += " FOR UPDATE"
	}
	var current int64
	if err = tx.QueryRowContext(ctx, query, key).Scan(&current); err != nil {
		return nil, err
	}
	var maximum sql.NullInt64
	if err = tx.QueryRowContext(ctx, "SELECT MAX("+quote(column)+") FROM "+tableSQL+" WHERE "+strings.Join(where, " AND "), args...).Scan(&maximum); err != nil {
		return nil, err
	}
	if maximum.Valid && maximum.Int64 > current {
		current = maximum.Int64
	}
	if current < 0 {
		current = 0
	}
	supplied := map[int64]bool{}
	for _, v := range r.Supplied {
		supplied[v] = true
	}
	values := make([]int64, 0, r.Count)
	for len(values) < r.Count {
		if current == math.MaxInt64 {
			return nil, fmt.Errorf("scoped sequence overflow")
		}
		current++
		if !supplied[current] {
			values = append(values, current)
		}
	}

	if _, err = tx.ExecContext(ctx, "UPDATE "+ledger+" SET value=? WHERE scope_key=?", current, key); err != nil {
		return nil, err
	}
	return values, nil
}
func singleIdentifier(value string) (string, error) {
	parts, err := sqlparser.TableIdentifierParts(value)
	if err != nil || len(parts) != 1 {
		return "", fmt.Errorf("invalid scoped sequence column %q", value)
	}
	return parts[0], nil
}

// Collision verifies that a generated scope/value was taken by another row and
// that the requested row identity is still absent. This prevents replay from
// turning a concurrent primary-key insert into an unintended UPDATE.
func Collision(ctx context.Context, db *sql.DB, dialect, table, column string, scope []Scope, value int64, identity []Scope) (bool, error) {
	if len(identity) == 0 || len(scope) == 0 {
		return false, fmt.Errorf("collision proof needs complete identity and scope")
	}
	quote := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	if strings.EqualFold(dialect, "mysql") {
		quote = func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	}
	parts, err := sqlparser.TableIdentifierParts(table)
	if err != nil || len(parts) == 0 || len(parts) > 2 {
		return false, fmt.Errorf("invalid table")
	}
	for i := range parts {
		parts[i] = quote(parts[i])
	}
	source := strings.Join(parts, ".")
	exists := func(values []Scope) (bool, error) {
		where := []string{}
		args := []any{}
		for _, v := range values {
			name, e := singleIdentifier(v.Column)
			if e != nil || v.Value == nil {
				return false, fmt.Errorf("invalid collision key")
			}
			where = append(where, quote(name)+"=?")
			args = append(args, v.Value)
		}
		var n int
		e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+source+" WHERE "+strings.Join(where, " AND "), args...).Scan(&n)
		return n > 0, e
	}
	present, err := exists(identity)
	if err != nil || present {
		return false, err
	}
	candidate := append([]Scope(nil), scope...)
	candidate = append(candidate, Scope{Column: column, Value: value})
	return exists(candidate)
}
