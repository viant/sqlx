package metadata

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

// Metadata namespaces are identifiers, while the remaining criteria are bound
// values. GoogleSQL requires one backtick pair around the complete path,
// including project names containing a hyphen followed by a digit.
var bigQueryMetadataSource = regexp.MustCompile(`(?:\$Args\[[0-9]+\]\.)+INFORMATION_SCHEMA\.[A-Z_]+`)

func expandBigQueryMetadataSources(sql string, args []interface{}, expanded []bool) (string, error) {
	dialect := &info.Dialect{}
	dialect.Name = "BigQuery"
	var firstErr error
	result := bigQueryMetadataSource.ReplaceAllStringFunc(sql, func(source string) string {
		parts := strings.Split(source, ".")
		path := make([]string, 0, len(parts))
		for _, part := range parts {
			if strings.HasPrefix(part, "$Args[") {
				index, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(part, "$Args["), "]"))
				if err != nil || index < 0 || index >= len(args) {
					firstErr = fmt.Errorf("missing BigQuery metadata namespace argument")
					return source
				}
				value, ok := args[index].(string)
				if !ok {
					firstErr = fmt.Errorf("BigQuery metadata namespace must be a string")
					return source
				}
				expanded[index] = true
				if value == "" {
					continue
				}
				// Validate each decoded namespace before joining. Do not interpolate
				// backticks, escapes, control delimiters or invalid UTF-8 into SQL.
				if _, err = dialect.QuoteIdentifier(value); err != nil {
					firstErr = fmt.Errorf("BigQuery metadata namespace: %w", err)
					return source
				}
				path = append(path, value)
			} else {
				path = append(path, part)
			}
		}
		quoted, err := dialect.QuoteIdentifier(strings.Join(path, "."))
		if err != nil {
			firstErr = err
			return source
		}
		return quoted
	})
	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}
