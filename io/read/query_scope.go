package read

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/viant/sqlx/io/read/cache"
)

var ErrQueryOutsideScope = errors.New("read query is outside the recorded execution scope")

// QueryScope is an immutable set of previously executed SQL/argument identities.
// It validates newly built queries; it never executes stored SQL or owns results.
// Identity requires byte-exact SQL and equal JSON-encoded arguments. This is
// deliberately stricter than the native cache hash, which normalizes SQL case
// and whitespace. QueryScope guards recorded execution identity, not cache keys.
type QueryScope struct{ queries map[string]map[string]bool }

func NewQueryScope(queries []cache.ParmetrizedQuery) (*QueryScope, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("recorded query scope is empty")
	}
	result := &QueryScope{queries: map[string]map[string]bool{}}
	for _, query := range queries {
		if query.SQL == "" {
			return nil, fmt.Errorf("recorded query SQL is empty")
		}
		args, err := json.Marshal(query.Args)
		if err != nil {
			return nil, err
		}
		values := result.queries[query.SQL]
		if values == nil {
			values = map[string]bool{}
			result.queries[query.SQL] = values
		}
		values[string(args)] = true
	}
	return result, nil
}
func (s *QueryScope) Validate(SQL string, args []interface{}) error {
	if s == nil {
		return nil
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if !s.queries[SQL][string(encoded)] {
		return ErrQueryOutsideScope
	}
	return nil
}
