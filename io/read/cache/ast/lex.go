package ast

import (
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	matcher2 "github.com/viant/sqlx/io/read/cache/ast/matcher"
)

const (
	whitespaceToken int = iota
	whitespaceTerminatorToken
	semicolonToken
	structToken
	ptrToken
	sliceToken

	tagToken

	interfaceToken

	intToken
	int8Token
	int16Token
	int32Token
	int64Token

	uintToken
	uint8Token
	uint16Token
	uint32Token
	uint64Token

	timeToken
	boolToken

	stringToken
	float32Token
	float64Token

	typeDefToken
	identityToken
)

var whitespaceMatcher = parsly.NewToken(whitespaceToken, "Whitespace", matcher.NewWhiteSpace())
var whitespaceTerminatorMatcher = parsly.NewToken(whitespaceTerminatorToken, "Whitespace terminator", matcher2.NewWhitespaceTerminator())
var semicolonMatcher = parsly.NewToken(semicolonToken, "Semicolon", matcher.NewByte(';'))
var tagMatcher = parsly.NewToken(tagToken, "Tag", matcher2.NewBlock('"', '"', '\\'))

var structMatcher = parsly.NewToken(structToken, "Struct", matcher.NewFragment("struct"))
var ptrMatcher = parsly.NewToken(ptrToken, "Pointer", matcher.NewByte('*'))
var sliceMatcher = parsly.NewToken(sliceToken, "Slice", matcher.NewFragment("[]"))
var interfaceMatcher = parsly.NewToken(interfaceToken, "Interface", matcher.NewFragment("interface {}"))

var intMatcher = parsly.NewToken(intToken, "Int", matcher.NewFragment("int"))
var int8Matcher = parsly.NewToken(int8Token, "Int8", matcher.NewFragment("int8"))
var int16Matcher = parsly.NewToken(int16Token, "Int16", matcher.NewFragment("int16"))
var int32Matcher = parsly.NewToken(int32Token, "Int32", matcher.NewFragment("int32"))
var int64Matcher = parsly.NewToken(int64Token, "Int64", matcher.NewFragment("int64"))

var uintMatcher = parsly.NewToken(uintToken, "uInt", matcher.NewFragment("uint"))
var uint8Matcher = parsly.NewToken(uint8Token, "uInt8", matcher.NewFragment("uint8"))
var uint16Matcher = parsly.NewToken(uint16Token, "uInt16", matcher.NewFragment("uint16"))
var uint32Matcher = parsly.NewToken(uint32Token, "uInt32", matcher.NewFragment("uint32"))
var uint64Matcher = parsly.NewToken(uint64Token, "uInt64", matcher.NewFragment("uint64"))

var float32Matcher = parsly.NewToken(float32Token, "Float32", matcher.NewFragment("float32"))
var float64Matcher = parsly.NewToken(float64Token, "Float64", matcher.NewFragment("float64"))

var stringMatcher = parsly.NewToken(stringToken, "String", matcher.NewFragment("string"))
var timeMatcher = parsly.NewToken(timeToken, "Time", matcher.NewFragment("time.Time"))
var boolMatcher = parsly.NewToken(boolToken, "Bool", matcher.NewFragment("bool"))

var typeDefMatcher = parsly.NewToken(typeDefToken, "Actual type", matcher.NewBlock('{', '}', '\\'))
var identityMatcher = parsly.NewToken(identityToken, "Identity", matcher2.NewIdentity())
