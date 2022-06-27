package criteria

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
)

func discoverAlias(cursor *parsly.Cursor) string {
	match := cursor.MatchAfterOptional(whitespaceToken, exceptKeywordToken, asKeywordToken, onKeywordToken, fromKeywordToken, joinToken, whereKeywordToken, groupByToken, havingKeywordToken, windowToken, identifierToken)
	switch match.Code {
	case asKeyword:
		match := cursor.MatchAfterOptional(whitespaceToken, identifierToken)
		return match.Text(cursor)
	case identifierCode:
		return match.Text(cursor)
	case exceptKeyword, fromKeyword, onKeyword, joinTokenCode, whereKeyword, groupByKeyword, havingKeyword, windowTokenCode:
		cursor.Pos -= match.Size
	}
	return ""
}

func expectOperand(cursor *parsly.Cursor) (node.Node, error) {

	literal, err := TryParseLiteral(cursor)
	if literal != nil || err != nil {
		return literal, err
	}

	match := cursor.MatchAfterOptional(whitespaceToken,
		asKeywordToken, onKeywordToken, fromKeywordToken, whereKeywordToken, joinToken, groupByToken, havingKeywordToken, windowToken, nextToken,
		parenthesesToken,
		caseBlockToken,
		starTokenToken,
		notOperatorToken,
		nullToken,
		selectorToken)
	switch match.Code {
	case selectorTokenCode:
		selector := expr.NewSelector(match.Text(cursor))
		match = cursor.MatchAfterOptional(whitespaceToken, parenthesesToken)
		if match.Code == parenthesesCode {
			return &expr.Call{X: selector, Raw: match.Text(cursor)}, nil
		}
		return selector, nil
	case nullTokenCode:
		return expr.NewNullLiteral(match.Text(cursor)), nil
	case caseBlock:
		return &expr.Switch{Raw: match.Text(cursor)}, nil
	case starTokenCode:
		return expr.NewSelector("*"), nil
	case parenthesesCode:
		return expr.NewParenthesis(match.Text(cursor)), nil
	case notOperator:
		unary := expr.NewUnary(match.Text(cursor))
		if unary.X, err = expectOperand(cursor); unary.X == nil || err != nil {
			return nil, cursor.NewError(selectorToken)
		}
		return unary, nil

	case asKeyword, onKeyword, fromKeyword, whereKeyword, joinTokenCode, groupByKeyword, havingKeyword, windowTokenCode, nextCode:
		cursor.Pos -= match.Size
	}
	return nil, nil
}
