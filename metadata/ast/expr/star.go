package expr

import "github.com/viant/sqlx/metadata/ast/node"

type Star struct {
	X      node.Node
	Except []string
}

func NewStar(x node.Node) *Star {
	return &Star{X: x}
}
