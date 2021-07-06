package database

import (
	"github.com/viant/parsly"
	"strings"
)

func Parse(input []byte) (info *Product, err error) {
	cursor := parsly.NewCursor("", input, 0)
	info = &Product{}

	if err = matchMarjoVersion(cursor, info);err != nil {
		return nil, err
	}
	matched := cursor.MatchOne(separator)
	if matched.Code != separator.Code {
		cursor.Pos++
		if err = matchMarjoVersion(cursor, info); err != nil {
			return info, nil
		}
		matched := cursor.MatchOne(separator)
		if matched.Code != separator.Code {
			return info, nil
		}
	}

	matched = cursor.MatchOne(digits)
	minor, _ := matched.Int(cursor)
	info.Minor = int(minor)

	matched = cursor.MatchOne(separator)
	if matched.Code != separator.Code {
		return info, nil
	}
	matched = cursor.MatchOne(digits)
	release, _ := matched.Int(cursor)
	info.Release = int(release)
	return info, nil
}


func matchMarjoVersion(cursor *parsly.Cursor, info *Product) error {
	matched := cursor.FindMatch(digits)
	if matched.Code != digits.Code {
		return  cursor.NewError(digits)
	}
	if matched.Offset > 0 {
		info.Name = strings.Trim(string(cursor.Input[:matched.Offset-1])," -\t\n")
	}
	major, _ := matched.Int(cursor)
	info.Major = int(major)
	return nil
}
