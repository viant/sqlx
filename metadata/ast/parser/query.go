package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/query"
	"strings"
)

func ParseQuery(SQL string) (*query.Select, error) {
	result := &query.Select{}
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	return result, parseQuery(cursor, result)
}

func parseQuery(cursor *parsly.Cursor, dest *query.Select) error {
	match := cursor.MatchAfterOptional(whitespaceToken, selectKeywordToken)
	switch match.Code {
	case selectKeyword:
		match = cursor.MatchAfterOptional(whitespaceToken, selectionKindToken)
		if match.Code == selectionKind {
			dest.Kind = match.Text(cursor)
		}
		dest.List = make(query.List, 0)
		if err := parseSelectListItem(cursor, &dest.List); err != nil {
			return err
		}
		match = cursor.MatchAfterOptional(whitespaceToken, fromKeywordToken)
		switch match.Code {
		case fromKeyword:
			dest.From = query.From{}
			match = cursor.MatchAfterOptional(whitespaceToken, selectorToken, parenthesesToken)
			switch match.Code {
			case selectorTokenCode:
				dest.From.X = expr.NewSelector(match.Text(cursor))
			case parenthesesCode:
				dest.From.X = expr.NewRaw(match.Text(cursor))
			}
			dest.From.Alias = discoverAlias(cursor)
			dest.Joins = make([]*query.Join, 0)

			match = cursor.MatchAfterOptional(whitespaceToken, joinToken, whereKeywordToken, groupByToken, havingKeywordToken, orderByKeywordToken, windowToken)
			if match.Code == parsly.EOF {
				return nil
			}
			hasMatch, err := matchPostFrom(cursor, dest, match)
			if !hasMatch && err == nil {
				err = cursor.NewError(joinToken, whereKeywordToken, groupByToken, havingKeywordToken, orderByKeywordToken, windowToken)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func matchPostFrom(cursor *parsly.Cursor, dest *query.Select, match *parsly.TokenMatch) (bool, error) {
	switch match.Code {
	case joinTokenCode:
		if err := appendJoin(cursor, match, dest); err != nil {
			return false, err
		}
	case whereKeyword:
		dest.Qualify = expr.NewQualify()
		if err := parseQualify(cursor, dest.Qualify); err != nil {
			return false, err
		}
		match = cursor.MatchAfterOptional(whitespaceToken, groupByToken, havingKeywordToken, orderByKeywordToken, windowToken)
		return matchPostFrom(cursor, dest, match)

	case groupByKeyword:
		if err := expectIdentifiers(cursor, &dest.GroupBy); err != nil {
			return false, err
		}
		match = cursor.MatchAfterOptional(whitespaceToken, havingKeywordToken, orderByKeywordToken, windowToken)
		return matchPostFrom(cursor, dest, match)

	case havingKeyword:
		dest.Having = expr.NewQualify()
		if err := parseQualify(cursor, dest.Having); err != nil {
			return false, err
		}
		match = cursor.MatchAfterOptional(whitespaceToken, orderByKeywordToken, windowToken)
		return matchPostFrom(cursor, dest, match)

	case orderByKeyword:
		if err := parseSelectListItem(cursor, &dest.OrderBy); err != nil {
			return false, err
		}
		match = cursor.MatchAfterOptional(whitespaceToken, windowToken)
		return matchPostFrom(cursor, dest, match)
	case windowTokenCode:
		matchedText := match.Text(cursor)
		dest.Window = expr.NewRaw(matchedText)
		match = cursor.MatchAfterOptional(whitespaceToken, intLiteralToken)
		if match.Code == intLiteral {
			literal := expr.NewNumericLiteral(match.Text(cursor))
			switch strings.ToLower(matchedText) {
			case "limit":
				dest.Limit = literal
			case "offset":
				dest.Offset = literal
			}
		}
	case parsly.EOF:
		return true, nil
	default:
		return false, nil
	}
	return true, nil
}

func expectExpectIdentifiers(cursor *parsly.Cursor, expect *[]string) (bool, error) {
	match := cursor.MatchAfterOptional(whitespaceToken, identifierToken)
	switch match.Code {
	case identifierCode:
		item := match.Text(cursor)
		*expect = append(*expect, item)
	default:
		return false, nil
	}

	snapshotPos := cursor.Pos
	match = cursor.MatchAfterOptional(whitespaceToken, nextToken)
	switch match.Code {
	case nextCode:
		has, err := expectExpectIdentifiers(cursor, expect)
		if err != nil {
			return false, err
		}
		if !has {
			cursor.Pos = snapshotPos
			return true, nil
		}
	}
	return true, nil
}

func expectIdentifiers(cursor *parsly.Cursor, expect *[]string) error {
	match := cursor.MatchAfterOptional(whitespaceToken, identifierToken)
	switch match.Code {
	case identifierCode:
		item := match.Text(cursor)
		*expect = append(*expect, item)
	default:
		return nil
	}

	match = cursor.MatchAfterOptional(whitespaceToken, nextToken)
	switch match.Code {
	case nextCode:
		return expectIdentifiers(cursor, expect)
	}
	return nil
}
