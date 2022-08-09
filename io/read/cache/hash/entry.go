package hash

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
)

func GenerateURL(SQL string, URL string, extension string, args []interface{}) (string, error) {
	argMarshal, err := json.Marshal(args)
	if err != nil {
		return "", err
	}

	return GenerateWithMarshal(SQL, URL, extension, argMarshal)
}

func GenerateWithMarshal(SQL string, URL string, extension string, argMarshal []byte) (string, error) {
	SQL = normalizeSQL(SQL)
	hasher := fnv.New64()
	_, err := hasher.Write(append([]byte(SQL), argMarshal...))
	if err != nil {
		return "", err
	}
	entryKey := strconv.Itoa(int(hasher.Sum64()))
	result := URL + entryKey + extension
	fmt.Printf("U:%v E:%v Ex:%v, S:%v\n", URL, entryKey, extension, SQL)
	return result, nil
}

func normalizeSQL(input string) string {
	var result = make([]byte, len(input))
	index := 0
	whiteSpaces := 0
	for i := range input {
		c := input[i]
		switch c {
		case ' ', '\t', '\r', '\n':
			if whiteSpaces == 0 {
				result[index] = ' '
				index++
			}
			whiteSpaces++
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			result[index] = c
			index++
		default:
			whiteSpaces = 0
			result[index] = c | ' '
			index++
		}
	}
	return string(result[:index])
}
