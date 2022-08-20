package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/node"
	"strings"
)

func discoverAlias(cursor *parsly.Cursor) string {
	match := cursor.MatchAfterOptional(whitespaceMatcher, exceptKeywordMatcher, asKeywordMatcher, onKeywordMatcher, fromKeywordMatcher, joinToken, whereKeywordMatcher, groupByMatcher, havingKeywordMatcher, orderByKeywordMatcher, windowMatcher, identifierMatcher)
	switch match.Code {
	case asKeyword:
		match := cursor.MatchAfterOptional(whitespaceMatcher, identifierMatcher)
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

	match := cursor.MatchAfterOptional(whitespaceMatcher,
		commentBlockMatcher,
		asKeywordMatcher, exceptKeywordMatcher, onKeywordMatcher, fromKeywordMatcher, whereKeywordMatcher, joinToken, groupByMatcher, havingKeywordMatcher, orderByKeywordMatcher, windowMatcher, nextMatcher,
		parenthesesMatcher,
		caseBlockMatcher,
		starTokenMatcher,
		notOperatorMatcher,
		nullMatcher,
		placeholderMatcher,
		selectorMatcher)
	switch match.Code {
	case selectorTokenCode, placeholderTokenCode:

		selRaw := match.Text(cursor)
		var selector node.Node
		selector = expr.NewSelector(selRaw)
		if match.Code == placeholderTokenCode {
			selector = expr.NewPlaceholder(selRaw)
		}
		match = cursor.MatchAfterOptional(whitespaceMatcher, parenthesesMatcher, exceptKeywordMatcher)
		switch match.Code {
		case parenthesesCode:
			return &expr.Call{X: selector, Raw: match.Text(cursor)}, nil
		case exceptKeyword:
			return parseStarExpr(cursor, selRaw, selector)
		}
		if strings.HasSuffix(selRaw, "*") {
			return expr.NewStar(selector), nil
		}
		return selector, nil
	case exceptKeyword:
		return nil, cursor.NewError(selectorMatcher)
	case nullTokenCode:
		return expr.NewNullLiteral(match.Text(cursor)), nil
	case caseBlock:
		return &expr.Switch{Raw: match.Text(cursor)}, nil
	case starTokenCode:
		selRaw := match.Text(cursor)
		selector := expr.NewSelector(selRaw)
		match = cursor.MatchAfterOptional(whitespaceMatcher, exceptKeywordMatcher)
		switch match.Code {
		case exceptKeyword:
			return parseStarExpr(cursor, selRaw, selector)
		}
		return expr.NewStar(selector), err
	case parenthesesCode:
		return expr.NewParenthesis(match.Text(cursor)), nil
	case notOperator:
		unary := expr.NewUnary(match.Text(cursor))
		if unary.X, err = expectOperand(cursor); unary.X == nil || err != nil {
			return nil, cursor.NewError(selectorMatcher)
		}
		return unary, nil

	case asKeyword, onKeyword, fromKeyword, whereKeyword, joinTokenCode, groupByKeyword, havingKeyword, windowTokenCode, nextCode, commentBlock:
		cursor.Pos -= match.Size
	}
	return nil, nil
}

func parseStarExpr(cursor *parsly.Cursor, selRaw string, selector node.Node) (node.Node, error) {
	star := expr.NewStar(selector)
	if !strings.HasSuffix(selRaw, "*") {
		return star, nil
	}
	_, err := expectExpectIdentifiers(cursor, &star.Except)
	return star, err
}
