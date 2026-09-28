package sqlx

import (
	"fmt"
	"strings"
)

// Criteria is a parameterized SQL predicate produced by a trusted compiler or
// predicate handler. It contains a Boolean expression, never a WHERE keyword.
// Driver placeholder rewriting and argument-count validation are shared by reads
// and mutations; clients must not supply arbitrary SQL through this contract.
type Criteria struct {
	Expression   string
	Placeholders []any
}

// Clone detaches the predicate's argument collection for one invocation.
func (c *Criteria) Clone() *Criteria {
	if c == nil {
		return nil
	}
	return &Criteria{Expression: c.Expression, Placeholders: append([]any(nil), c.Placeholders...)}
}

// SQL rewrites executable positional parameters using the caller's current
// dialect position. SQLparser's scanner preserves quoted/comment question marks.
func (c *Criteria) SQL(next func() string) (string, error) {
	if c == nil || strings.TrimSpace(c.Expression) == "" {
		if c != nil && len(c.Placeholders) != 0 {
			return "", fmt.Errorf("empty predicate has placeholders")
		}
		return "", nil
	}
	parameters := ParseParameters(c.Expression)
	if parameters.NamedCount() != 0 {
		return "", fmt.Errorf("predicate requires positional placeholders")
	}
	if parameters.PositionalCount() != len(c.Placeholders) {
		return "", fmt.Errorf("predicate placeholders: expression=%d, values=%d", parameters.PositionalCount(), len(c.Placeholders))
	}
	return parameters.RewritePositional(func(int) string { return next() }), nil
}
