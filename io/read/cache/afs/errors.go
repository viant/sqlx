package afs

import (
	"net/http"
	"strconv"
	"strings"
)

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
