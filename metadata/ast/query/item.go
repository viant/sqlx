package query

import (
	"github.com/viant/sqlx/metadata/ast/node"
)

type Item struct {
	Expr     node.Node
	Alias    string
	DataType string
	Raw      string
	Meta     interface{}
}

func NewItem(expr node.Node) *Item {
	return &Item{Expr: expr}
}
