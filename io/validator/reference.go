package validator

import (
	"fmt"
	"slices"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/info"
)

// MatchesTarget compares explicit logical identity without changing the SQL or
// stored receipt descriptor. No default project/schema or case folding is inferred.
func (r Reference) MatchesTarget(other Reference, dialect *info.Dialect) (bool, error) {
	if r.Field != other.Field || r.Column != other.Column {
		return false, nil
	}
	if r.Schema == other.Schema && r.Table == other.Table {
		return true, nil
	}
	left, err := r.tableParts(dialect)
	if err != nil {
		return false, err
	}
	right, err := other.tableParts(dialect)
	if err != nil {
		return false, err
	}
	return slices.Equal(left, right), nil
}

func (r Reference) tableParts(dialect *info.Dialect) ([]string, error) {
	product := ""
	if dialect != nil {
		product = dialect.Name
	}
	parser := sqlparser.TableIdentifierParser{Product: product}
	var parts []string
	if r.Schema != "" {
		schema, err := parser.Parts(r.Schema)
		if err != nil {
			return nil, err
		}
		parts = append(parts, schema...)
	}
	table, err := parser.Parts(r.Table)
	if err != nil {
		return nil, err
	}
	parts = append(parts, table...)
	if strings.EqualFold(product, "BigQuery") && len(parts) > 3 {
		return nil, fmt.Errorf("BigQuery reference has more than three qualified parts")
	}
	return parts, nil
}
