package parser

import (
	"bytes"
	"fmt"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/sqlx/metadata/ast/query"
)

func Stringify(n node.Node) string {
	builder := new(bytes.Buffer)
	stringify(n, builder)
	return builder.String()
}

func stringify(n node.Node, builder *bytes.Buffer) {
	switch actual := n.(type) {
	case *query.Select:
		builder.WriteString("SELECT ")
		stringify(actual.List, builder)
		builder.WriteString(" FROM ")
		stringify(&actual.From, builder)

		if len(actual.Joins) > 0 {
			for _, join := range actual.Joins {
				stringify(join, builder)
			}
		}
		if actual.Qualify != nil {
			builder.WriteString(" WHERE ")
			stringify(actual.Qualify.X, builder)
		}
	case *query.Join:
		builder.WriteByte(' ')
		builder.WriteString(actual.Raw)
		builder.WriteByte(' ')
		stringify(actual.With, builder)
		if actual.Alias != "" {
			builder.WriteByte(' ')
			builder.WriteString(actual.Alias)
		}
		if actual.Comments != "" {
			builder.WriteString(" " + actual.Comments)
		}
		builder.WriteString(" ON ")
		stringify(actual.On, builder)
	case *expr.Qualify:
		stringify(actual.X, builder)

	case *expr.Literal:
		builder.WriteString(actual.Value)
	case query.List:
		listSize := len(actual)
		if listSize == 0 {
			return
		}
		stringify(actual[0], builder)
		for i := 1; i < listSize; i++ {
			builder.WriteString(", ")
			stringify(actual[i], builder)
		}

	case *expr.Star:
		stringify(actual.X, builder)
		if len(actual.Except) > 0 {
			builder.WriteString(" EXCEPT ")
			for i, item := range actual.Except {
				if i > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(item)
			}
		}

	case *expr.Raw:
		builder.WriteString(" ")
		builder.WriteString(actual.Raw)
		builder.WriteString(" ")
	case *query.From:
		stringify(actual.X, builder)
		if actual.Alias != "" {
			builder.WriteString(" " + actual.Alias)
		}
	case *expr.Placeholder:
		builder.WriteString(actual.Name)
	case *expr.Unary:
		builder.WriteString(" " + actual.Op + " ")
		stringify(actual.X, builder)
	case *expr.Parenthesis:
		builder.WriteString(actual.Raw)
	case *query.Item:
		stringify(actual.Expr, builder)
		if actual.Alias != "" {
			builder.WriteString(" AS " + actual.Alias)
		}
		if actual.Comments != "" {
			builder.WriteString(" " + actual.Comments)
		}
	case *expr.Binary:
		stringify(actual.X, builder)
		builder.WriteString(" " + actual.Op + " ")
		if actual.Y != nil {
			stringify(actual.Y, builder)
		}
	case expr.Raw:
		builder.WriteString(actual.Raw)
	case *expr.Ident:
		builder.WriteString(actual.Name)
	case *expr.Call:
		stringify(actual.X, builder)
		builder.WriteString(actual.Raw)
	case *expr.Range:
		stringify(actual.Min, builder)
		builder.WriteString(" AND ")
		stringify(actual.Max, builder)
	case *expr.Selector:
		builder.WriteString(actual.Name)
		if actual.X != nil {
			builder.WriteByte('.')
		}
		stringify(actual.X, builder)
	default:
		panic(fmt.Sprintf("%T unsupported", n))
	}

}
