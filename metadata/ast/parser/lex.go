package parser

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	smatcher "github.com/viant/sqlx/metadata/ast/matcher"

	"github.com/viant/parsly/matcher/option"
)

type Token int

const (
	whitespaceCode int = iota
	parenthesesCode
	nextCode
	identifierCode
	starTokenCode
	nullTokenCode
	notOperator
	binaryOperator
	logicalOperator
	intLiteral
	numericLiteral
	boolLiteral
	nullKeyword
	singleQuotedStringLiteral
	doubleQuotedStringLiteral
	caseBlock
	commentBlock
	selectKeyword
	selectorTokenCode
	asKeyword
	selectionKindCode
	exceptKeyword
	selectionKind
	fromKeyword
	onKeyword
	joinTokenCode
	whereKeyword
	groupByKeyword
	havingKeyword
	orderByKeyword
	windowTokenCode
	literalCode
)

var whitespaceToken = parsly.NewToken(whitespaceCode, "whitespace", matcher.NewWhiteSpace())
var parenthesesToken = parsly.NewToken(parenthesesCode, "()", matcher.NewBlock('(', ')', '\\'))
var nextToken = parsly.NewToken(nextCode, ",", matcher.NewByte(','))
var asKeywordToken = parsly.NewToken(asKeyword, "AS", matcher.NewFragment("as", &option.Case{}))
var starTokenToken = parsly.NewToken(starTokenCode, "*", matcher.NewFragment("*", &option.Case{}))
var notOperatorToken = parsly.NewToken(notOperator, "NOT", matcher.NewFragment("not", &option.Case{}))
var nullToken = parsly.NewToken(nullTokenCode, "NULL", matcher.NewFragment("null", &option.Case{}))
var selectionKindToken = parsly.NewToken(selectionKindCode, "ALL|DISTINCT|STRUCT", matcher.NewSet([]string{
	"ALL", "DISTINCT", "STRUCT",
}, &option.Case{}))
var caseBlockToken = parsly.NewToken(caseBlock, "CASE", matcher.NewSeqBlock("CASE", "END"))
var commentBlockToken = parsly.NewToken(commentBlock, "/* */", matcher.NewSeqBlock("/*", "*/"))

var selectKeywordToken = parsly.NewToken(selectKeyword, "SELECT", matcher.NewFragment("select", &option.Case{}))
var exceptKeywordToken = parsly.NewToken(exceptKeyword, "EXCEPT", matcher.NewFragment("except", &option.Case{}))

var fromKeywordToken = parsly.NewToken(fromKeyword, "FROM", matcher.NewFragment("from", &option.Case{}))
var joinToken = parsly.NewToken(joinTokenCode, "LEFT OUTER JOIN|LEFT JOIN|JOIN", matcher.NewSpacedSet([]string{
	"left outer join",
	"left join",
	"join",
}, &option.Case{}))

var onKeywordToken = parsly.NewToken(onKeyword, "ON", matcher.NewFragment("on", &option.Case{}))

var whereKeywordToken = parsly.NewToken(whereKeyword, "WHERE", matcher.NewFragment("where", &option.Case{}))
var groupByToken = parsly.NewToken(groupByKeyword, "GROUP BY", matcher.NewSpacedFragment("group by", &option.Case{}))
var havingKeywordToken = parsly.NewToken(havingKeyword, "HAVING", matcher.NewFragment("having", &option.Case{}))

var orderByKeywordToken = parsly.NewToken(orderByKeyword, "ORDER BY", matcher.NewSpacedFragment("order by", &option.Case{}))
var windowToken = parsly.NewToken(windowTokenCode, "LIMIT|OFFSET", matcher.NewSet([]string{"limit", "offset"}, &option.Case{}))

var binaryOperatorToken = parsly.NewToken(binaryOperator, "binary OPERATOR", matcher.NewSpacedSet([]string{"+", "!=", "=", "-", ">", "<", "=>", "=<", "*", "/", "in", "not in", "is not", "is"}, &option.Case{}))
var logicalOperatorToken = parsly.NewToken(logicalOperator, "AND|OR", matcher.NewSet([]string{"and", "or"}, &option.Case{}))

var nullKeywordToken = parsly.NewToken(nullKeyword, "NULL", matcher.NewFragment("null", &option.Case{}))
var boolLiteralToken = parsly.NewToken(boolLiteral, "true|false", matcher.NewSet([]string{"true", "false"}, &option.Case{}))
var singleQuotedStringLiteralToken = parsly.NewToken(singleQuotedStringLiteral, `'...'`, matcher.NewByteQuote('\'', '\\'))
var doubleQuotedStringLiteralToken = parsly.NewToken(doubleQuotedStringLiteral, `"..."`, matcher.NewByteQuote('\'', '\\'))
var intLiteralToken = parsly.NewToken(intLiteral, `INT`, smatcher.NewIntMatcher())
var numericLiteralToken = parsly.NewToken(numericLiteral, `NUMERIC`, matcher.NewNumber())

var identifierToken = parsly.NewToken(identifierCode, "IDENT", smatcher.NewIdentifier())
var selectorToken = parsly.NewToken(selectorTokenCode, "SELECTOR", smatcher.NewSelector())
var literalToken = parsly.NewToken(literalCode, "LITERAL", matcher.NewNop())
