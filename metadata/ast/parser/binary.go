package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
)

func parseBinaryExpr(cursor *parsly.Cursor, binary *expr.Binary) error {
	var err error
	if binary.X == nil {
		binary.X, err = expectOperand(cursor)
		if err != nil || binary.X == nil {
			return err
		}
	}
	if binary.Op == "" {
		match := cursor.MatchAfterOptional(whitespaceMatcher, betweenKeywordMatcher, binaryOperatorMatcher, logicalOperatorMatcher)
		switch match.Code {
		case binaryOperator, logicalOperator:
			binary.Op = match.Text(cursor)
		case betweenToken:
			binary.Op = match.Text(cursor)
			rng := &expr.Range{}
			if rng.Min, err = expectOperand(cursor); err != nil {
				return err
			}
			match := cursor.MatchAfterOptional(whitespaceMatcher, rangeOperatorMatcher)
			if match.Code != rangeOperator {
				return cursor.NewError(rangeOperatorMatcher)
			}
			if rng.Max, err = expectOperand(cursor); err != nil {
				return err
			}
			yExpr := &expr.Binary{X: rng}
			if err := parseBinaryExpr(cursor, yExpr); err != nil {
				return err
			}
			if yExpr.Y == nil {
				binary.Y = rng
			} else {
				binary.Y = yExpr
			}
			return nil
		default:
			return nil
		}
	}
	if binary.Y == nil {
		yExpr := &expr.Binary{}
		if err := parseBinaryExpr(cursor, yExpr); err != nil {
			return err
		}
		if yExpr.X != nil {
			binary.Y = yExpr
		}
		if yExpr.Op == "" {
			binary.Y = yExpr.X
		}
	}
	return nil
}
