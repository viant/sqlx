package io

import "strings"

func byLowerCase(key string) string {
	return strings.ToLower(key)
}

func fuzzyKey(key string) string {
	lowerCased := strings.ToLower(key)
	count := strings.Count(lowerCased, "_")
	if count == 0 {
		return lowerCased
	}
	return strings.Replace(lowerCased, "_", "", count)
}
