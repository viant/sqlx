package cache

//there probably won't be more types read from the file when parsing it to the []interface{}{}
func normalizeInt(val interface{}) (int64, bool) {
	switch actual := val.(type) {
	case float64:
		return int64(actual), true
	case float32:
		return int64(actual), true
	case int:
		return int64(actual), true
	case uint:
		return int64(actual), true
	case int64:
		return actual, true
	}

	return 0, false
}
