package insert

import "github.com/viant/sqlx/metadata/ast/node"

type Value struct {
	Expr     node.Node
	Comments string
	Raw      string
	Meta     interface{}
}
