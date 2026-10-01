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
	"sync"

	"github.com/viant/sqlparser"
)

// Transient keeps caller-owned, process-local scoped counters. It never creates
// database objects, inserts placeholders, or completes the caller transaction.
// Independent owners/processes rely on the source UNIQUE constraint and their
// generated-only mutation retry. Allocated values retain rollback gaps.
type Transient struct{ states sync.Map }
type transientKey struct {
	db    *sql.DB
	scope [32]byte
}
type transientState struct {
	mu          sync.Mutex
	initialized bool
	current     int64
}

func (s *Transient) Reserve(ctx context.Context, db *sql.DB, tx *sql.Tx, r Request) ([]int64, error) {
	if s == nil || db == nil || tx == nil {
		return nil, fmt.Errorf("transient scoped allocation requires a database and caller transaction")
	}
	if r.Count < 0 || len(r.Scope) == 0 {
		return nil, fmt.Errorf("transient scoped allocation requires scope and nonnegative count")
	}
	dialect := strings.ToLower(r.Dialect)
	if dialect != "mysql" && dialect != "sqlite" && dialect != "sqlite3" {
		return nil, fmt.Errorf("transient scoped allocation unsupported for dialect %q", r.Dialect)
	}
	parts, err := sqlparser.TableIdentifierParts(r.Table)
	if err != nil || len(parts) < 1 || len(parts) > 2 {
		return nil, fmt.Errorf("invalid scoped source table")
	}
	column, err := singleIdentifier(r.Column)
	if err != nil {
		return nil, err
	}
	quote := func(v string) string { return `"` + strings.ReplaceAll(v, `"`, `""`) + `"` }
	if dialect == "mysql" {
		quote = func(v string) string { return "`" + strings.ReplaceAll(v, "`", "``") + "`" }
	}
	table := make([]string, len(parts))
	for i, p := range parts {
		table[i] = quote(p)
	}
	scope := append([]Scope(nil), r.Scope...)
	sort.Slice(scope, func(i, j int) bool { return strings.ToLower(scope[i].Column) < strings.ToLower(scope[j].Column) })
	canonical := []any{strings.Join(parts, "."), strings.ToLower(column)}
	where := []string{}
	args := []any{}
	seen := map[string]bool{}
	for _, item := range scope {
		name, e := singleIdentifier(item.Column)
		if e != nil || item.Value == nil || seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("transient scope requires distinct nonnull physical columns")
		}
		switch item.Value.(type) {
		case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		default:
			return nil, fmt.Errorf("invalid transient scoped value")
		}
		seen[strings.ToLower(name)] = true
		where = append(where, quote(name)+"=?")
		args = append(args, item.Value)
		canonical = append(canonical, strings.ToLower(name), item.Value)
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	key := transientKey{db: db, scope: sha256.Sum256(encoded)}
	stored, _ := s.states.LoadOrStore(key, &transientState{})
	state := stored.(*transientState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !state.initialized {
		var maximum sql.NullInt64
		if err = tx.QueryRowContext(ctx, "SELECT MAX("+quote(column)+") FROM "+strings.Join(table, ".")+" WHERE "+strings.Join(where, " AND "), args...).Scan(&maximum); err != nil {
			return nil, err
		}
		if maximum.Valid && maximum.Int64 > 0 {
			state.current = maximum.Int64
		}
		state.initialized = true
	}
	supplied := map[int64]bool{}
	for _, value := range r.Supplied {
		supplied[value] = true
	}
	values := make([]int64, 0, r.Count)
	for len(values) < r.Count {
		if state.current == math.MaxInt64 {
			return nil, fmt.Errorf("transient scoped sequence overflow")
		}
		state.current++
		if !supplied[state.current] {
			values = append(values, state.current)
		}
	}
	return values, nil
}
