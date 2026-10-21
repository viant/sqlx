package schema

import (
	"fmt"
	"strings"
)

type sqlToken struct {
	word       string
	start, end int
}

// SplitSQL reads SQL, including quoted identifiers/strings, comments, PostgreSQL
// dollar quotes, and SQLite trigger BEGIN/END bodies. It rejects client commands
// (DELIMITER, GO, backslash commands), COPY FROM STDIN and unquoted procedural
// blocks outside SQLite triggers. PostgreSQL strings assume standard_conforming_strings
// is on; use E'...' for backslash escapes. MySQL strings use its default escape
// rules. Script settings that change lexical rules require explicit statements.
func SplitSQL(script string, dialect string) ([]string, error) {
	tokens, err := lexSQL(script, dialect)
	if err != nil {
		return nil, err
	}
	var result []string
	start := 0
	var words []string
	trigger, body := false, false
	cases := 0
	previous := ""
	meaningful := false
	appendStatement := func(end int) {
		if meaningful {
			result = append(result, strings.TrimSpace(script[start:end]))
		}
		meaningful = false
		words = nil
		trigger = false
		body = false
		cases = 0
		previous = ""
	}
	for _, tok := range tokens {
		word := tok.word
		prior := previous
		previous = word
		if word == ";" {
			if !body {
				appendStatement(tok.start)
				start = tok.end
			}
			continue
		}
		meaningful = true
		if len(words) < 8 {
			words = append(words, word)
		}
		if len(words) == 1 {
			if word == "<mysql-comment>" {
				return nil, scriptError(script, tok.start, "standalone MySQL executable comments require explicit ordinary SQL")
			}
			if word == "DELIMITER" || word == "\\" || word == "." || (word == "GO" && onlyOnLine(script, tok)) {
				return nil, scriptError(script, tok.start, "unsupported SQL client directive "+word)
			}
		}
		if word == "GO" && onlyOnLine(script, tok) {
			return nil, scriptError(script, tok.start, "unsupported SQL client directive GO")
		}
		if len(words) > 0 && words[0] == "SET" && (word == "SQL_MODE" || word == "STANDARD_CONFORMING_STRINGS") {
			return nil, scriptError(script, tok.start, "changing SQL lexical settings inside schema scripts is unsupported")
		}
		if len(words) > 0 && words[0] == "COPY" && (word == "STDIN" || word == "STDOUT") {
			return nil, scriptError(script, tok.start, "COPY streaming client protocol is unsupported")
		}
		if strings.EqualFold(dialect, "sqlite") && len(words) >= 2 && words[0] == "CREATE" {
			second := 1
			if words[1] == "TEMP" || words[1] == "TEMPORARY" {
				second = 2
			}
			if len(words) > second && words[second] == "TRIGGER" {
				trigger = true
			}
		}
		if word == "BEGIN" {
			if trigger {
				body = true
			} else if proceduralSQL(words) {
				return nil, scriptError(script, tok.start, "unquoted procedural block is unsupported; use a dollar-quoted body")
			}
		}
		if body {
			if word == "CASE" {
				cases++
			}
			if word == "END" {
				if cases > 0 {
					cases--
				} else if prior == ";" {
					body = false
				}
			}
		}
	}
	if body {
		return nil, scriptError(script, len(script), "unterminated SQLite trigger body")
	}
	appendStatement(len(script))
	return result, nil
}

func proceduralSQL(words []string) bool {
	if len(words) == 0 {
		return false
	}
	if words[0] == "DO" {
		return true
	}
	if words[0] != "CREATE" {
		return false
	}
	for _, word := range words {
		switch word {
		case "FUNCTION", "PROCEDURE", "TRIGGER", "EVENT":
			return true
		}
	}
	return false
}

func onlyOnLine(script string, tok sqlToken) bool {
	begin := strings.LastIndex(script[:tok.start], "\n") + 1
	end := strings.IndexByte(script[tok.end:], '\n')
	if end < 0 {
		end = len(script)
	} else {
		end += tok.end
	}
	return strings.TrimSpace(script[begin:tok.start]) == "" && strings.TrimSpace(script[tok.end:end]) == ""
}

func scriptError(script string, at int, reason string) error {
	return fmt.Errorf("SQL script line %d: %s", strings.Count(script[:at], "\n")+1, reason)
}
func sqlWord(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c >= 128
}

func lexSQL(script, dialect string) ([]sqlToken, error) {
	var result []sqlToken
	mysql := strings.EqualFold(dialect, "mysql")
	bq := strings.EqualFold(dialect, "bigquery")
	pg := strings.EqualFold(dialect, "postgresql") || strings.EqualFold(dialect, "pg") || strings.EqualFold(dialect, "postgres")
	for i := 0; i < len(script); {
		c := script[i]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == '\f' {
			i++
			continue
		}
		if c == '-' && i+1 < len(script) && script[i+1] == '-' && (!mysql || i+2 == len(script) || script[i+2] <= ' ') {
			i += 2
			for i < len(script) && script[i] != '\n' {
				i++
			}
			continue
		}
		if (mysql || bq) && c == '#' {
			for i < len(script) && script[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(script) && script[i+1] == '*' {
			begin := i
			executable := mysql && i+2 < len(script) && script[i+2] == '!'
			i += 2
			depth := 1
			for i < len(script) && depth > 0 {
				if pg && i+1 < len(script) && script[i:i+2] == "/*" {
					depth++
					i += 2
					continue
				}
				if i+1 < len(script) && script[i:i+2] == "*/" {
					depth--
					i += 2
					continue
				}
				if executable && script[i] == ';' {
					return nil, scriptError(script, i, "semicolon in MySQL executable comment is unsupported")
				}
				i++
			}
			if depth != 0 {
				return nil, scriptError(script, begin, "unterminated comment")
			}
			if executable {
				result = append(result, sqlToken{word: "<mysql-comment>", start: begin, end: i})
			}
			continue
		}
		begin := i
		if c == '\'' || c == '"' || c == '`' || c == '[' && !pg && !mysql && !bq {
			end := c
			// GoogleSQL triple-quoted strings can contain unescaped newlines,
			// quotes and semicolons. Raw prefixes retain escaped delimiters.
			if bq && (c == '\'' || c == '"') && i+2 < len(script) && script[i+1] == c && script[i+2] == c {
				delimiter := script[i : i+3]
				i += 3
				closed := false
				for i < len(script) {
					if script[i] == '\\' {
						i += 2
						continue
					}
					if strings.HasPrefix(script[i:], delimiter) {
						i += 3
						closed = true
						break
					}
					i++
				}
				if !closed {
					return nil, scriptError(script, begin, "unterminated triple quote")
				}
				result = append(result, sqlToken{word: "<quoted>", start: begin, end: i})
				continue
			}
			if c == '[' {
				end = ']'
			}
			i++
			closed := false
			escape := mysql && c != '`' || bq || pg && c == '\'' && begin > 0 && (script[begin-1] == 'E' || script[begin-1] == 'e') && (begin == 1 || !sqlWord(script[begin-2]))
			for i < len(script) {
				if escape && script[i] == '\\' {
					i += 2
					continue
				}
				if script[i] == end {
					if c != '[' && i+1 < len(script) && script[i+1] == end {
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
				return nil, scriptError(script, begin, "unterminated quote")
			}
			result = append(result, sqlToken{word: "<quoted>", start: begin, end: i})
			continue
		}
		if pg && c == '$' {
			end := i + 1
			if end < len(script) && script[end] != '$' && (script[end] >= '0' && script[end] <= '9') {
				end = i
			}
			if end > i {
				for end < len(script) && sqlWord(script[end]) {
					end++
				}
			}
			if end > i && end < len(script) && script[end] == '$' {
				delimiter := script[i : end+1]
				close := strings.Index(script[end+1:], delimiter)
				if close < 0 {
					return nil, scriptError(script, i, "unterminated dollar quote")
				}
				i = end + 1 + close + len(delimiter)
				result = append(result, sqlToken{word: "<quoted>", start: begin, end: i})
				continue
			}
		}
		if sqlWord(c) {
			i++
			for i < len(script) && (sqlWord(script[i]) || script[i] == '$') {
				i++
			}
			result = append(result, sqlToken{word: strings.ToUpper(script[begin:i]), start: begin, end: i})
			continue
		}
		i++
		result = append(result, sqlToken{word: string(c), start: begin, end: i})
	}
	return result, nil
}
