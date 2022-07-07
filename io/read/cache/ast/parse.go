package ast

import (
	"github.com/viant/parsly"
	"reflect"
	"strconv"
)

type (
	TypeDef struct {
		Name string
		Type reflect.Type
	}

	Modifier func(p reflect.Type) reflect.Type
)

func Parse(dataType string, extraTypes ...reflect.Type) (reflect.Type, error) {
	typesIndex := map[string]reflect.Type{}
	for i, extraType := range extraTypes {
		typesIndex[extraType.String()] = extraTypes[i]
	}

	cursor := parsly.NewCursor("", []byte(dataType), 0)

	rType, err := matchType(cursor, typesIndex)
	if err != nil {
		return nil, err
	}

	return rType, nil
}

func matchType(cursor *parsly.Cursor, typesIndex map[string]reflect.Type) (reflect.Type, error) {
	candidates := []*parsly.Token{
		uint64Matcher, uint32Matcher, uint16Matcher, uint8Matcher, uintMatcher,
		int64Matcher, int32Matcher, int16Matcher, int8Matcher, intMatcher, stringMatcher, float32Matcher, float64Matcher,
		structMatcher, timeMatcher, whitespaceTerminatorMatcher,
	}

	modifiers := getModifiers(cursor)
	var rType reflect.Type
	matched := cursor.MatchAfterOptional(whitespaceMatcher, candidates...)
	switch matched.Code {
	case intToken:
		rType = IntType
	case stringToken:
		rType = StringType
	case float32Token:
		rType = Float32Type
	case float64Token:
		rType = Float64Type
	case int8Token:
		rType = Int8Type
	case int16Token:
		rType = Int16Type
	case int32Token:
		rType = Int32Type
	case int64Token:
		rType = Int64Type
	case uintToken:
		rType = UintType
	case uint8Token:
		rType = Uint8Type
	case uint16Token:
		rType = Uint16Type
	case uint32Token:
		rType = Uint32Type
	case uint64Token:
		rType = Uint64Type
	case structToken:
		structType, err := matchStruct(cursor, typesIndex)
		if err != nil {
			return nil, err
		}
		rType = structType
	case timeToken:
		rType = TimeType
	case whitespaceTerminatorToken:
		candidate := matched.Text(cursor)
		candidateType, ok := typesIndex[candidate]

		if !ok {
			return nil, cursor.NewError(candidates...)
		}

		rType = candidateType
	case parsly.Invalid:
		return nil, cursor.NewError(candidates...)
	}

	for i := len(modifiers) - 1; i >= 0; i-- {
		rType = modifiers[i](rType)
	}
	return rType, nil
}

func matchStruct(cursor *parsly.Cursor, index map[string]reflect.Type) (reflect.Type, error) {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, typeDefMatcher)
	if matched.Code != typeDefToken {
		return nil, cursor.NewError(typeDefMatcher)
	}

	bytes := matched.Bytes(cursor)
	actualDef := bytes[1 : len(bytes)-1]
	newCursor := parsly.NewCursor("", actualDef, 0)
	return buildStruct(newCursor, index)
}

func buildStruct(newCursor *parsly.Cursor, index map[string]reflect.Type) (reflect.Type, error) {
	structFields := []reflect.StructField{}

	i := 0
	var matched *parsly.TokenMatch
	for newCursor.Pos < newCursor.InputSize {
		if i != 0 {
			matched = newCursor.MatchAfterOptional(whitespaceMatcher, semicolonMatcher)
			switch matched.Code {
			case parsly.Invalid:
				return nil, newCursor.NewError(semicolonMatcher)
			}
		}

		field := reflect.StructField{}
		matched = newCursor.MatchAfterOptional(whitespaceMatcher, identityMatcher, semicolonMatcher)
		switch matched.Code {
		case parsly.EOF:
			return reflect.StructOf(structFields), nil
		case parsly.Invalid, semicolonToken:
			return nil, newCursor.NewError(identityMatcher)
		}

		field.Name = matched.Text(newCursor)
		if field.Name[0] < 'A' || field.Name[0] > 'Z' {
			field.PkgPath = "github.com/viant/sqlx/io/read/data/ast"
		}

		fieldType, err := matchType(newCursor, index)
		if err != nil {
			return nil, err
		}

		aTag, ok, err := matchStructTag(newCursor)
		if err != nil {
			return nil, err
		}

		if ok {
			field.Tag = aTag
		}

		field.Type = fieldType
		i++
		structFields = append(structFields, field)
	}

	return reflect.StructOf(structFields), nil
}

func matchStructTag(cursor *parsly.Cursor) (reflect.StructTag, bool, error) {
	matched := cursor.MatchAfterOptional(whitespaceMatcher, tagMatcher)
	if matched.Code != tagToken {
		return "", true, nil
	}

	text, err := strconv.Unquote(matched.Text(cursor))
	if err != nil {
		return "", false, err
	}

	return reflect.StructTag(text), true, nil
}

func getModifiers(cursor *parsly.Cursor) []Modifier {
	var modifiers []Modifier
outer:
	for {
		matched := cursor.MatchAfterOptional(whitespaceMatcher, ptrMatcher, sliceMatcher)
		switch matched.Code {
		case ptrToken:
			modifiers = append(modifiers, reflect.PtrTo)
		case sliceToken:
			modifiers = append(modifiers, reflect.SliceOf)
		default:
			break outer
		}
	}

	return modifiers
}
