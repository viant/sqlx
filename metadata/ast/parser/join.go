package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/query"
)

func parseJoin(cursor *parsly.Cursor, join *query.Join, dest *query.Select) error {
	match := cursor.MatchAfterOptional(whitespaceToken, parenthesesToken, selectorToken)
	switch match.Code {
	case parenthesesCode:
		join.With = expr.NewRaw(match.Text(cursor))
	case selectorTokenCode:
		join.With = expr.NewSelector(match.Text(cursor))
	}

	join.Alias = discoverAlias(cursor)

	match = cursor.MatchAfterOptional(whitespaceToken, onKeywordToken)
	switch match.Code {
	case onKeyword:
	default:
		return cursor.NewError(onKeywordToken)
	}
	binary := &expr.Binary{}
	join.On = &expr.Qualify{}
	join.On.X = binary
	if err := parseBinaryExpr(cursor, binary); err != nil {
		return err
	}
	match = cursor.MatchAfterOptional(whitespaceToken, joinToken, groupByToken, havingKeywordToken, whereKeywordToken, windowToken)
	if match.Code == parsly.EOF {
		return nil
	}
	hasMatch, err := matchPostFrom(cursor, dest, match)
	if !hasMatch && err == nil {
		err = cursor.NewError(joinToken, joinToken, groupByToken, havingKeywordToken, whereKeywordToken, windowToken)
	}
	return err
}

func appendJoin(cursor *parsly.Cursor, match *parsly.TokenMatch, dest *query.Select) error {
	join := query.NewJoin(match.Text(cursor))
	dest.Joins = append(dest.Joins, join)
	if err := parseJoin(cursor, join, dest); err != nil {
		return err
	}
	return nil
}
