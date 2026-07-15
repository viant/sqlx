package cache

import (
	"fmt"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

type warmupAndTerm struct {
	node   node.Node
	binary *expr.Binary
}

func deriveWarmupIdentityFromSQL(SQL string, args []interface{}, by string) (string, []interface{}, bool, string) {
	if strings.TrimSpace(by) == "" {
		return "", nil, false, "by_empty"
	}

	parsed, err := sqlparser.ParseQuery(SQL)
	if err != nil {
		return "", nil, false, "parse_error"
	}
	target, ok, reason := warmupIdentityTarget(parsed)
	if !ok {
		return "", nil, false, reason
	}
	if target.Qualify == nil || target.Qualify.X == nil {
		return "", nil, false, "unsupported_where_shape"
	}

	terms, ok := topLevelAndTerms(target.Qualify.X)
	if !ok || len(terms) == 0 {
		return "", nil, false, "unsupported_where_shape"
	}

	normalizedBy := normalizeWarmupIdentifier(by)
	selectorNames, selectorExprs := warmupSelectorNames(target, normalizedBy)
	selectorIndex := -1
	selectorArgStart := 0
	selectorArgCount := 0
	totalPlaceholders := 0
	argOffset := 0

	for i, term := range terms {
		placeholderCount := countNodePlaceholders(term.node)
		totalPlaceholders += placeholderCount
		if term.binary == nil {
			argOffset += placeholderCount
			continue
		}
		if hasNestedLogicalBinary(term.binary.X) || hasNestedLogicalBinary(term.binary.Y) {
			argOffset += placeholderCount
			continue
		}

		ident, values, err := safeBinaryPredicate(term.binary)
		if err == nil && ident != nil && isWarmupSelectorMatch(ident, selectorNames, selectorExprs) {
			if values == nil || !supportsWarmupSelector(term.binary.Op, values) {
				if placeholderCount == 0 {
					argOffset += placeholderCount
					continue
				}
				return "", nil, false, "unsupported_selector_predicate"
			}
			if selectorIndex != -1 {
				return "", nil, false, "unsupported_selector_predicate"
			}
			selectorIndex = i
			selectorArgStart = argOffset
			selectorArgCount = placeholderCount
		}

		argOffset += placeholderCount
	}

	if selectorIndex == -1 || selectorArgCount == 0 || totalPlaceholders != len(args) {
		switch {
		case selectorIndex == -1:
			return "", nil, false, "selector_not_found"
		case selectorArgCount == 0:
			return "", nil, false, "selector_without_placeholders"
		default:
			return "", nil, false, "placeholder_mismatch"
		}
	}

	remaining := make([]node.Node, 0, len(terms)-1)
	for i, term := range terms {
		if i == selectorIndex {
			continue
		}
		remaining = append(remaining, term.node)
	}

	if len(remaining) == 0 {
		target.Qualify = nil
	} else {
		target.Qualify = &expr.Qualify{X: joinAndTerms(remaining)}
	}
	target.Limit = nil
	target.Offset = nil
	if parsed.IsNested() {
		parsed.OrderBy = nil
		parsed.Limit = nil
		parsed.Offset = nil
		if isWrapperPaginationArtifact(parsed) {
			parsed.Window = nil
		}
		if raw, ok := parsed.From.X.(*expr.Raw); ok {
			raw.Raw = "(" + sqlparser.Stringify(target) + ")"
			raw.Unparsed = ""
		}
	}

	return sqlparser.Stringify(parsed), removeArgsRange(args, selectorArgStart, selectorArgCount), true, "applied"
}

func hasNestedLogicalBinary(n node.Node) bool {
	switch actual := n.(type) {
	case *expr.Parenthesis:
		if actual.X == nil {
			return false
		}
		return hasNestedLogicalBinary(actual.X)
	case *expr.Binary:
		if strings.EqualFold(strings.TrimSpace(actual.Op), "AND") || strings.EqualFold(strings.TrimSpace(actual.Op), "OR") {
			return true
		}
		return hasNestedLogicalBinary(actual.X) || hasNestedLogicalBinary(actual.Y)
	default:
		return false
	}
}

func warmupSelectorNames(target *query.Select, normalizedBy string) (map[string]bool, map[string]bool) {
	result := map[string]bool{
		normalizedBy: true,
	}
	exprs := map[string]bool{}
	if target == nil {
		return result, exprs
	}
	for _, item := range target.List {
		if item == nil || strings.TrimSpace(item.Alias) == "" || item.Expr == nil {
			continue
		}
		if normalizeWarmupIdentifier(item.Alias) != normalizedBy {
			continue
		}
		exprs[normalizeWarmupExpression(sqlparser.Stringify(item.Expr))] = true
	}
	return result, exprs
}

func isWarmupSelectorMatch(ident node.Node, selectorNames map[string]bool, selectorExprs map[string]bool) bool {
	if ident == nil {
		return false
	}
	if selectorNames[normalizeWarmupIdentifier(sqlparser.Stringify(ident))] {
		return true
	}
	return selectorExprs[normalizeWarmupExpression(sqlparser.Stringify(ident))]
}

func warmupIdentityTarget(sel *query.Select) (*query.Select, bool, string) {
	if !supportsWarmupIdentityDerivation(sel) {
		return nil, false, "unsupported_query_shape"
	}

	if sel.IsNested() {
		nested := materializeNestedSelect(sel)
		if nested == nil || !supportsNestedWarmupIdentityDerivation(sel, nested) {
			return nil, false, "unsupported_query_shape"
		}
		return nested, true, ""
	}

	return sel, true, ""
}

func supportsWarmupIdentityDerivation(sel *query.Select) bool {
	if sel == nil {
		return false
	}
	if sel.Union != nil || sel.Having != nil {
		return false
	}
	return true
}

func supportsNestedWarmupIdentityDerivation(outer *query.Select, nested *query.Select) bool {
	if outer == nil || nested == nil {
		return false
	}
	if outer.Qualify != nil || outer.Having != nil || outer.Union != nil || len(outer.WithSelects) > 0 {
		return false
	}
	if len(outer.Joins) > 0 || len(outer.GroupBy) > 0 {
		return false
	}
	if outer.Window != nil && !isWrapperPaginationArtifact(outer) {
		return false
	}
	if nested.Union != nil || nested.Having != nil {
		return false
	}
	return true
}

func isWrapperPaginationArtifact(sel *query.Select) bool {
	if sel == nil || sel.Window == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(sel.Window.Raw), "LIMIT") {
		return false
	}
	return sel.Limit != nil || len(sel.OrderBy) > 0 || sel.Offset != nil
}

func topLevelAndTerms(n node.Node) ([]warmupAndTerm, bool) {
	switch actual := n.(type) {
	case *expr.Parenthesis:
		if actual.X == nil {
			inner, ok := materializeParenthesisNode(actual)
			if !ok {
				return []warmupAndTerm{{node: actual}}, true
			}
			if binary, _ := inner.(*expr.Binary); binary != nil {
				return []warmupAndTerm{{node: actual, binary: binary}}, true
			}
			return []warmupAndTerm{{node: actual}}, true
		}
		terms, ok := topLevelAndTerms(actual.X)
		if !ok {
			return nil, false
		}
		if len(terms) == 1 && !isAndExpression(actual.X) {
			terms[0].node = actual
		}
		return terms, true
	case *expr.Binary:
		if strings.TrimSpace(actual.Op) == "" && actual.Y == nil && actual.X != nil {
			return topLevelAndTerms(actual.X)
		}
		if strings.EqualFold(strings.TrimSpace(actual.Op), "AND") {
			left, ok := topLevelAndTerms(actual.X)
			if !ok {
				return nil, false
			}
			right, ok := topLevelAndTerms(actual.Y)
			if !ok {
				return nil, false
			}
			return append(left, right...), true
		}
		if nested, ok := actual.Y.(*expr.Binary); ok && strings.EqualFold(strings.TrimSpace(nested.Op), "AND") {
			left, ok := topLevelAndTerms(&expr.Binary{X: actual.X, Op: actual.Op, Y: nested.X})
			if !ok {
				return nil, false
			}
			right, ok := topLevelAndTerms(nested.Y)
			if !ok {
				return nil, false
			}
			return append(left, right...), true
		}
		return []warmupAndTerm{{node: actual, binary: actual}}, true
	default:
		return nil, false
	}
}

func isAndExpression(n node.Node) bool {
	actual, ok := n.(*expr.Binary)
	if !ok || actual == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(actual.Op), "AND") {
		return true
	}
	normalized, ok := safeNormalizeBinary(actual)
	return ok && normalized != nil && strings.EqualFold(strings.TrimSpace(normalized.Op), "AND")
}

func safeNormalizeBinary(actual *expr.Binary) (result *expr.Binary, ok bool) {
	defer func() {
		if recover() != nil {
			result = nil
			ok = false
		}
	}()
	return actual.Normalize(), true
}

func safeBinaryPredicate(actual *expr.Binary) (ident node.Node, values *expr.Values, err error) {
	defer func() {
		if recover() != nil {
			ident = nil
			values = nil
			err = fmt.Errorf("predicate panic")
		}
	}()
	return actual.Predicate()
}

func supportsWarmupSelector(operator string, values *expr.Values) bool {
	switch strings.ToUpper(strings.TrimSpace(operator)) {
	case "=", "IN":
	default:
		return false
	}
	for _, value := range values.X {
		if !value.Placeholder {
			return false
		}
	}
	return true
}

func countPlaceholderValues(values *expr.Values) int {
	result := 0
	for _, value := range values.X {
		if value.Placeholder {
			result++
		}
	}
	return result
}

func countNodePlaceholders(n node.Node) int {
	return strings.Count(sqlparser.Stringify(n), "?")
}

func joinAndTerms(terms []node.Node) node.Node {
	if len(terms) == 1 {
		return terms[0]
	}
	result := terms[0]
	for i := 1; i < len(terms); i++ {
		result = &expr.Binary{
			X:  result,
			Op: "AND",
			Y:  terms[i],
		}
	}
	return result
}

func materializeParenthesisNode(actual *expr.Parenthesis) (node.Node, bool) {
	if actual == nil {
		return nil, false
	}
	if actual.X != nil {
		return actual.X, true
	}
	raw := strings.TrimSpace(actual.Raw)
	if raw == "" {
		return nil, false
	}
	parsed, err := sqlparser.ParseQuery("SELECT * FROM t WHERE " + raw)
	if err != nil || parsed == nil || parsed.Qualify == nil || parsed.Qualify.X == nil {
		return nil, false
	}
	return parsed.Qualify.X, true
}

func materializeNestedSelect(sel *query.Select) *query.Select {
	if sel == nil {
		return nil
	}
	if nested := sel.NestedSelect(); nested != nil {
		return nested
	}
	raw, ok := sel.From.X.(*expr.Raw)
	if !ok || raw == nil {
		return nil
	}
	text := strings.TrimSpace(raw.Raw)
	if strings.HasPrefix(text, "(") && strings.HasSuffix(text, ")") {
		text = strings.TrimSpace(text[1 : len(text)-1])
	}
	parsed, err := sqlparser.ParseQuery(text)
	if err != nil {
		return nil
	}
	return parsed
}

func removeArgsRange(args []interface{}, start, count int) []interface{} {
	if count == 0 {
		return args
	}
	result := make([]interface{}, 0, len(args)-count)
	result = append(result, args[:start]...)
	result = append(result, args[start+count:]...)
	return result
}

func normalizeWarmupIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.LastIndex(value, "."); index != -1 {
		value = value[index+1:]
	}
	return strings.ToLower(value)
}

func normalizeWarmupExpression(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
