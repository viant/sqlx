package cache

import (
	"bufio"
	"net/http"
	"strconv"
	"strings"
)

func readLine(reader *bufio.Reader) ([]byte, error) {
	line, prefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	var restLine []byte
	for prefix {
		restLine, prefix, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, restLine...)
	}

	return line, nil
}

func isRateError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), strconv.Itoa(http.StatusTooManyRequests))
}

func isPreConditionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), strconv.Itoa(http.StatusPreconditionFailed))
}
