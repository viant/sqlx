package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
)

func parseQualify(cursor *parsly.Cursor, qualify *expr.Qualify) error {
	binary := &expr.Binary{}
	err := parseBinaryExpr(cursor, binary)
	qualify.X = binary
	return err
}
