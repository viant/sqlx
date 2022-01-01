package generators

func sqlValue(defaultValue, columnName string, placeholderGenerator func() string) string {
	return "COALESCE(" + placeholderGenerator() + ", " + defaultValue + ") AS " + columnName
}
