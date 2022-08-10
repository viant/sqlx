package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/update"
)

func ParseUpdate(SQL string) (*update.Statement, error) {
	result := &update.Statement{}
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	return result, parseUpdate(cursor, result)

}

func parseUpdate(cursor *parsly.Cursor, stmt *update.Statement) error {

	match := cursor.MatchAfterOptional(whitespaceMatcher, updateKeywordMatcher)
	switch match.Code {
	case updateKeyword:
		match = cursor.MatchAfterOptional(whitespaceMatcher, selectorMatcher)
		switch match.Code {
		case selectorTokenCode:
			sel := match.Text(cursor)
			stmt.Target.X = expr.NewSelector(sel)
			match = cursor.MatchAfterOptional(whitespaceMatcher, commentBlockMatcher)
			if match.Code == commentBlock {
				stmt.Target.Comments = match.Text(cursor)
			}
			match = cursor.MatchAfterOptional(whitespaceMatcher, setKeywordMatcher)
			if match.Code != setKeyword {
				return cursor.NewError(setKeywordMatcher)
			}

			item, err := expectUpdateSetItem(cursor)
			if err != nil {
				return err
			}
			stmt.Set = append(stmt.Set, item)
			if err = parseUpdateSetItems(cursor, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseUpdateSetItems(cursor *parsly.Cursor, stmt *update.Statement) error {
	match := cursor.MatchAfterOptional(whitespaceMatcher, whereKeywordMatcher, nextMatcher)
	switch match.Code {
	case whereKeyword:
		stmt.Qualify = &expr.Qualify{}
		if err := ParseQualify(cursor, stmt.Qualify); err != nil {
			return err
		}
	case nextCode:
		item, err := expectUpdateSetItem(cursor)
		if err != nil {
			return err
		}
		stmt.Set = append(stmt.Set, item)
		return parseUpdateSetItems(cursor, stmt)
	case parsly.EOF:
	default:
		return cursor.NewError(nextMatcher, whereKeywordMatcher)
	}
	return nil
}

func expectUpdateSetItem(cursor *parsly.Cursor) (*update.Item, error) {
	match := cursor.MatchAfterOptional(whitespaceMatcher, selectorMatcher)
	if match.Code != selectorTokenCode {
		return nil, cursor.NewError(selectorMatcher)
	}
	selRaw := match.Text(cursor)
	item := &update.Item{Column: expr.NewSelector(selRaw)}
	match = cursor.MatchAfterOptional(whitespaceMatcher, assignOperatorMatcher)
	if match.Code != assignOperator {
		return nil, cursor.NewError(assignOperatorMatcher)
	}
	operand, err := expectOperand(cursor)
	if err != nil {
		return nil, err
	}
	item.Expr = operand
	return item, err
}
