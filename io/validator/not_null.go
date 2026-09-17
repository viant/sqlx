package validator

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/viant/sqlx/io"
)

// checkNotNull validates the SQL value, not Go zero-ness. In particular false,
// zero and an empty string satisfy NOT NULL. The canonical column binder keeps
// embedded-holder and encoding semantics identical to native DML.
func (s *Service) checkNotNull(ctx context.Context, path *Path, at io.ValueAccessor, count int, checks *Checks, result *Validation, options *Options) error {
	if len(checks.NoNull) == 0 {
		return nil
	}
	parameter := make([]interface{}, 1)
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		record := at(i)
		for _, check := range checks.NoNull {
			if options.deferredAt(i, check.Field.Name) || !options.includesAt(i, record, check.Field.Name) {
				continue
			}
			checks.bind(record, parameter, check.columnIndex, 1)
			value, err := driver.DefaultParameterConverter.ConvertValue(parameter[0])
			if err != nil {
				return fmt.Errorf("validate %s: %w", path.AppendIndex(i).AppendField(check.Field.Name).String(), err)
			}
			isNull := value == nil
			if bytes, ok := value.([]byte); ok && bytes == nil {
				isNull = true
			}
			if isNull {
				result.AppendNotNull(path.AppendIndex(i).AppendField(check.Field.Name), check.Field.Name, check.ErrorMsg)
			}
		}
	}
	return nil
}
