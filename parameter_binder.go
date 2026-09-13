package sqlx

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

// ParameterResolver resolves one named SQL value from an invocation scope.
type ParameterResolver func(name string) (any, bool, error)

// ParameterBinder binds successive SQL fragments while preserving consumption
// order of the shared positional arguments. Complete verifies full consumption.
type ParameterBinder struct {
	resolver   ParameterResolver
	positional []any
	position   int
}

func NewParameterBinder(resolver ParameterResolver, positional ...any) *ParameterBinder {
	return &ParameterBinder{resolver: resolver, positional: append([]any(nil), positional...)}
}

func (b *ParameterBinder) Bind(SQL string) (string, []any, error) {
	if b == nil {
		return "", nil, fmt.Errorf("parameter binder is required")
	}
	parameters := ParseParameters(SQL)
	var result strings.Builder
	var args []any
	previous, position := 0, b.position
	for _, item := range parameters.items {
		var value any
		if item.name == "" {
			if position >= len(b.positional) {
				return "", nil, fmt.Errorf("missing positional SQL argument %d", position+1)
			}
			value = b.positional[position]
			position++
		} else {
			if b.resolver == nil {
				return "", nil, fmt.Errorf("parameter resolver is required for named placeholder %s", item.name)
			}
			var found bool
			var err error
			value, found, err = b.resolver(item.name)
			if err != nil {
				return "", nil, fmt.Errorf("resolve SQL parameter %q: %w", item.name, err)
			}
			if !found {
				return "", nil, fmt.Errorf("missing named SQL parameter %q", item.name)
			}
		}
		result.WriteString(SQL[previous:item.start])
		values := b.values(value)
		if len(values) == 0 {
			result.WriteString("NULL")
		} else {
			for i, value := range values {
				if i > 0 {
					result.WriteByte(',')
				}
				result.WriteByte('?')
				args = append(args, value)
			}
		}
		previous = item.end
	}
	result.WriteString(SQL[previous:])
	b.position = position
	return result.String(), args, nil
}

func (b *ParameterBinder) Complete() error {
	if b == nil {
		return fmt.Errorf("parameter binder is required")
	}
	if b.position != len(b.positional) {
		return fmt.Errorf("SQL consumed %d positional arguments but %d were supplied", b.position, len(b.positional))
	}
	return nil
}

func (b *ParameterBinder) values(value any) []any {
	if value == nil {
		return []any{nil}
	}
	if _, ok := value.(driver.Valuer); ok {
		return []any{value}
	}
	actual := reflect.ValueOf(value)
	for actual.Kind() == reflect.Pointer {
		if actual.IsNil() {
			return []any{nil}
		}
		actual = actual.Elem()
	}
	if (actual.Kind() != reflect.Slice && actual.Kind() != reflect.Array) || actual.Type().Elem().Kind() == reflect.Uint8 {
		return []any{value}
	}
	result := make([]any, actual.Len())
	for i := range result {
		result[i] = actual.Index(i).Interface()
	}
	return result
}
