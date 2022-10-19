package query

import (
	"github.com/viant/sqlx/metadata/ast/expr"
)

type (
	Select struct {
		List        List
		From        From
		Joins       []*Join
		Qualify     *expr.Qualify
		GroupBy     List
		Having      *expr.Qualify
		OrderBy     List
		Window      *expr.Raw
		Limit       *expr.Literal
		Offset      *expr.Literal
		Kind        string
		Union       *Union
		WithSelects []*WithSelect
	}

	WithSelect struct {
		Alias string
		X     *Select
	}

	Union struct {
		Kind string
		Raw  string
		X    *Select
	}
)
