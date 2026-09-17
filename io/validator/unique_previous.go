package validator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata/info"
)

type uniqueCandidate struct {
	path      *Path
	value     driver.Value
	args      []interface{}
	SQL       string
	duplicate bool
}
type uniqueCandidateKey struct{ value, dependency interface{} }
type uniqueBytesKey string

// checkUniquePrevious keeps each candidate's exclusions inside that candidate's
// query. A shared exclusion set would hide collisions with another updated row.
func (s *Service) checkUniquePrevious(ctx context.Context, path *Path, db *sql.DB, at io.ValueAccessor, count int, check *Check, result *Validation, options *Options) error {
	previous := func(int) interface{} { return nil }
	if options.candidatePoliciesSet {
		previous = func(index int) interface{} { return options.CandidatePolicies[index].Previous }
	} else if options.Previous != nil {
		var n int
		var err error
		previous, n, err = io.Values(options.Previous)
		if err != nil {
			return fmt.Errorf("previous rows: %w", err)
		}
		if n != count {
			return fmt.Errorf("previous row count %d does not match candidate count %d", n, count)
		}
	}
	var dialect *info.Dialect
	var err error
	var keyIndexes []int
	var keyNames []string
	for i, column := range check.columns {
		if tag := column.Tag(); tag != nil && tag.PrimaryKey {
			keyIndexes = append(keyIndexes, i)
			keyNames = append(keyNames, column.Name())
		}
	}
	var candidates []*uniqueCandidate
	index := make(map[uniqueCandidateKey]*uniqueCandidate)
	dependencyIndex := -1
	dependencyField := ""
	if check.UniqueDep != nil {
		for j, column := range check.columns {
			if column.Name() == (*check.UniqueDep).Name() {
				dependencyIndex = j
				fields := column.(io.Fielder).Fields()
				dependencyField = fields[len(fields)-1].Name
				break
			}
		}
	}
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if options.deferredAt(i, check.Field.Name) || (dependencyIndex >= 0 && options.deferredAt(i, dependencyField)) {
			continue
		}
		current := at(i)
		prior := previous(i)
		if !options.candidatePoliciesSet {
			prior, err = check.previousRow(current, prior, i)
			if err != nil {
				return err
			}
		}
		valueSet, dependencySet := options.includesAt(i, current, check.Field.Name), true
		if dependencyIndex >= 0 {
			dependencySet = options.includesAt(i, current, dependencyField)
		}
		if !valueSet && (dependencyIndex < 0 || !dependencySet) {
			continue
		}
		if dialect == nil {
			if db == nil {
				return fmt.Errorf("unique validation requires a database")
			}
			dialect, err = options.resolveDialect(ctx, db)
			if err != nil {
				return err
			}
		}
		valueSource := current
		if !valueSet {
			valueSource = prior
		}
		if valueSource == nil {
			return fmt.Errorf("candidate %d omitted unique field %s without previous value", i, check.Field.Name)
		}
		value, err := check.sqlValue(valueSource, check.columnIndex)
		if err != nil {
			return err
		}
		if value == nil {
			continue
		}
		candidate := &uniqueCandidate{path: path.AppendIndex(i).AppendField(check.Field.Name), value: value, SQL: check.SQL + " = ?", args: []interface{}{value}}
		key := uniqueCandidateKey{value: check.comparableValue(value)}
		if dependencyIndex >= 0 {
			column := check.columns[dependencyIndex]
			dependencySource := current
			if !dependencySet {
				dependencySource = prior
			}
			if dependencySource == nil {
				return fmt.Errorf("candidate %d omitted unique dependency %s without previous value", i, dependencyField)
			}
			dependency, err := check.sqlValue(dependencySource, dependencyIndex)
			if err != nil {
				return err
			}
			// A nullable member does not participate in SQL UNIQUE equality.
			if dependency == nil {
				continue
			}
			candidate.SQL += " AND " + column.Name() + " = ?"
			candidate.args = append(candidate.args, dependency)
			key.dependency = check.comparableValue(dependency)
		}
		if prior != nil {
			if len(keyIndexes) == 0 {
				return fmt.Errorf("previous row %d has no declared primary key", i)
			}
			candidate.SQL += " AND NOT (" + dialect.CompositeIn(keyNames, 1) + ")"
			for _, j := range keyIndexes {
				identity, err := check.sqlValue(prior, j)
				if err != nil {
					return err
				}
				if identity == nil {
					return fmt.Errorf("previous row %d primary key %s is null", i, check.columns[j].Name())
				}
				candidate.args = append(candidate.args, identity)
			}
		}
		limit := options.MaxPlaceholders
		if limit <= 0 {
			limit = dialect.MaxPlaceholderCount()
		}
		if limit > 0 && len(candidate.args) > limit {
			return fmt.Errorf("unique validation requires %d placeholders, limit is %d", len(candidate.args), limit)
		}
		if first := index[key]; first != nil {
			first.duplicate = true
			candidate.duplicate = true
		} else {
			index[key] = candidate
		}
		candidates = append(candidates, candidate)
	}
	// Construct and validate every candidate before executing any query.
	for _, candidate := range candidates {
		if candidate.duplicate {
			result.AppendUnique(candidate.path, check.Field.Name, candidate.value, check.ErrorMsg)
			continue
		}
		reader, err := options.reader(ctx, db, candidate.SQL, check.CheckType)
		if err != nil {
			return err
		}
		found := false
		err = reader.QueryAll(ctx, func(interface{}) error { found = true; return nil }, candidate.args...)
		if statement := reader.Stmt(); statement != nil {
			_ = statement.Close()
		}
		if err != nil {
			return err
		}
		if found {
			result.AppendUnique(candidate.path, check.Field.Name, candidate.value, check.ErrorMsg)
		}
	}
	return nil
}

func (c *Check) previousRow(current, previous interface{}, index int) (interface{}, error) {
	if previous == nil {
		return nil, nil
	}
	currentType, previousType := reflect.TypeOf(current), reflect.TypeOf(previous)
	if currentType.Kind() == reflect.Ptr {
		currentType = currentType.Elem()
	}
	if previousType.Kind() == reflect.Ptr {
		previousType = previousType.Elem()
	}
	if currentType != previousType {
		return nil, fmt.Errorf("previous row %d type %v does not match %v", index, previousType, currentType)
	}
	value := reflect.ValueOf(previous)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return nil, nil
	}
	return previous, nil
}

func (c *Check) sqlValue(record interface{}, index int) (driver.Value, error) {
	parameter := make([]interface{}, 1)
	c.bind(record, parameter, index, 1)
	value, err := driver.DefaultParameterConverter.ConvertValue(parameter[0])
	if err != nil {
		return nil, fmt.Errorf("validate column %s: %w", c.columns[index].Name(), err)
	}
	if bytes, ok := value.([]byte); ok && bytes == nil {
		return nil, nil
	}
	return value, nil
}

func (c *Check) comparableValue(value driver.Value) interface{} {
	if bytes, ok := value.([]byte); ok {
		return uniqueBytesKey(bytes)
	}
	return value
}
