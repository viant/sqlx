package sequence

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// currentAutoValue reads the engine's SHOW CREATE output, not the cached
// INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT statistic. No session setting or
// source DDL is changed to refresh metadata.
func (m *ReservationMetadata) currentAutoValue(ctx context.Context, q queryer, source, mode string) (uint64, error) {
	var name, ddl string
	if err := q.QueryRowContext(ctx, "SHOW CREATE TABLE "+source).Scan(&name, &ddl); err != nil {
		return 0, err
	}
	return m.tableAutoValue(ddl, mode)
}

// tableAutoValue decodes the table option in engine-produced DDL. The existing
// MySQL adapter already reads SHOW CREATE; this lexer excludes column bodies,
// quoted identifiers, string comments and version comments from option matching.
func (*ReservationMetadata) tableAutoValue(ddl, mode string) (uint64, error) {
	depth := 0
	body := false
	noEscapes := strings.Contains(mode, "NO_BACKSLASH_ESCAPES")
	ansi := strings.Contains(mode, "ANSI_QUOTES")
	for i := 0; i < len(ddl); {
		ch := ddl[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			quote := ch
			identifier := quote == '`' || quote == '"' && ansi
			i++
			closed := false
			for i < len(ddl) {
				if ddl[i] == '\\' && !identifier && !noEscapes {
					i += 2
					continue
				}
				if ddl[i] == quote {
					if i+1 < len(ddl) && ddl[i+1] == quote {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return 0, fmt.Errorf("unterminated quote in MySQL SHOW CREATE metadata")
			}
			continue
		}
		if i+1 < len(ddl) && ddl[i:i+2] == "/*" {
			end := strings.Index(ddl[i+2:], "*/")
			if end < 0 {
				return 0, fmt.Errorf("unterminated MySQL metadata comment")
			}
			i += end + 4
			continue
		}
		if ch == '(' {
			depth++
			i++
			continue
		}
		if ch == ')' {
			depth--
			if depth == 0 {
				body = true
			}
			if depth < 0 {
				return 0, fmt.Errorf("invalid MySQL metadata parentheses")
			}
			i++
			continue
		}
		if ch == '_' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' {
			start := i
			i++
			for i < len(ddl) && (ddl[i] == '_' || ddl[i] >= 'A' && ddl[i] <= 'Z' || ddl[i] >= 'a' && ddl[i] <= 'z' || ddl[i] >= '0' && ddl[i] <= '9') {
				i++
			}
			if body && depth == 0 && strings.EqualFold(ddl[start:i], "AUTO_INCREMENT") {
				for i < len(ddl) && (ddl[i] == ' ' || ddl[i] == '\n' || ddl[i] == '\t') {
					i++
				}
				if i >= len(ddl) || ddl[i] != '=' {
					return 0, fmt.Errorf("invalid AUTO_INCREMENT metadata option")
				}
				i++
				for i < len(ddl) && (ddl[i] == ' ' || ddl[i] == '\t') {
					i++
				}
				start = i
				for i < len(ddl) && ddl[i] >= '0' && ddl[i] <= '9' {
					i++
				}
				value, err := strconv.ParseUint(ddl[start:i], 10, 64)
				if err != nil || value == 0 {
					return 0, fmt.Errorf("invalid AUTO_INCREMENT metadata value %q", ddl[start:i])
				}
				return value, nil
			}
			continue
		}
		i++
	}
	if !body || depth != 0 {
		return 0, fmt.Errorf("invalid MySQL SHOW CREATE table metadata")
	}
	return 1, nil
}
