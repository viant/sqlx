package read

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
	goIo "io"
	"reflect"
)

type Rows struct {
	cleanupErrorProvenance bool
	rows                   *sql.Rows
	columns                []io.Column
	xTypes                 []*xunsafe.Type
	cache                  cache.Cache
	entry                  *cache.Entry
	matcher                *cache.ParmetrizedQuery
	occurIndex             map[interface{}]int
	columnIndex            int
	matcherColumnDerefs    []*xunsafe.Type
	exhausted              int
}

// CleanupError identifies an actually returned close failure, preserving its
// original display text and causes. It grants no continuation authority.
type CleanupError struct {
	Cause   error
	message string
}

func (e *CleanupError) Error() string { return e.message }
func (e *CleanupError) Unwrap() error { return e.Cause }

func cleanupError(cause error) error {
	if cause == nil {
		return nil
	}
	var existing *CleanupError
	if errors.As(cause, &existing) {
		return cause
	}
	return &CleanupError{Cause: cause, message: cause.Error()}
}

func (c *Rows) Rollback(ctx context.Context) error {
	if c.cache != nil && c.entry != nil {
		_ = c.cache.Rollback(ctx, c.entry)
	}

	if c.rows != nil {
		return c.rows.Close()
	}

	return nil
}

func (c *Rows) CheckType(ctx context.Context, values []interface{}) (bool, error) {
	return true, nil
}

func NewRows(rows *sql.Rows, cache cache.Cache, entry *cache.Entry, matcher *cache.ParmetrizedQuery) (*Rows, error) {
	readerRows := &Rows{
		rows:        rows,
		cache:       cache,
		entry:       entry,
		matcher:     matcher,
		occurIndex:  map[interface{}]int{},
		columnIndex: -1,
	}

	return readerRows, nil
}

func (c *Rows) Err() error {
	return c.rows.Err()
}

func (c *Rows) ConvertColumns() ([]io.Column, error) {
	if len(c.columns) == 0 {
		if err := c.initColumns(); err != nil {
			return nil, err
		}
	}

	return c.columns, nil
}

func (c *Rows) Scanner(ctx context.Context) cache.ScannerFn {
	return func(args ...interface{}) error {
		if c.matcher != nil && len(c.matcher.In) > 0 && len(c.matcher.In) == c.exhausted {
			return goIo.EOF
		}

		var err error
		if err = c.rows.Scan(args...); err != nil {
			return err
		}

		if !(c.columnIndex == -1 || c.matcher == nil) {
			columnValue := c.asKey(args[c.columnIndex])
			occurTimes := c.occurIndex[columnValue] + 1
			limitReached := (occurTimes-c.matcher.Offset) > c.matcher.Limit && c.matcher.Limit != 0

			if !limitReached {
				c.occurIndex[columnValue] = occurTimes
			}

			if (occurTimes <= c.matcher.Offset && c.matcher.Offset != 0) || limitReached {
				return SkipError("skipped")
			}

			if occurTimes == c.matcher.Limit && c.matcher.Limit != 0 {
				c.exhausted++
			}
		}

		if err = c.rows.Err(); err != nil {
			return err
		}

		var ok bool
		if c.entry != nil {
			ok, err = c.cache.UpdateType(ctx, c.entry, args)
			if !ok {
				c.entry = nil
				c.cache = nil
			}

			if err != nil {
				return err
			}
		}

		return nil
	}
}

func (c *Rows) XTypes() []*xunsafe.Type {
	if c.xTypes != nil {
		return c.xTypes
	}

	c.xTypes = make([]*xunsafe.Type, len(c.columns))
	for i, column := range c.columns {
		c.xTypes[i] = xunsafe.NewType(column.ScanType())
	}

	return c.xTypes
}

func (c *Rows) initColumns() error {
	columnNames, err := c.rows.Columns()
	if err != nil {
		return err
	}

	c.columns = io.NamesToColumns(columnNames)
	if columnsTypes, _ := c.rows.ColumnTypes(); len(columnNames) > 0 {
		c.columns = io.TypesToColumns(columnsTypes)
	}

	return c.initMatcherColumn()
}

func (c *Rows) initXTypes() {
	c.xTypes = make([]*xunsafe.Type, len(c.columns))
	for i, column := range c.columns {
		c.xTypes[i] = xunsafe.NewType(column.ScanType())
	}
}

func (c *Rows) Close(ctx context.Context) error {
	var failures []error
	if c.entry != nil {
		if err := c.cache.Close(ctx, c.entry); err != nil {
			failures = append(failures, err)
		}
	}

	if err := c.rows.Close(); err != nil {
		failures = append(failures, err)
	}

	if len(failures) == 0 {
		return nil
	}

	var errMessage string
	for _, err := range failures {
		errMessage += err.Error()
	}

	if c.cleanupErrorProvenance {
		return &CleanupError{Cause: errors.Join(failures...), message: errMessage}
	}
	return fmt.Errorf("%s", errMessage)
}

func (c *Rows) Next() bool {
	return c.rows.Next()
}

func (c *Rows) initMatcherColumn() error {
	if c.matcher == nil {
		return nil
	}

	if len(c.matcher.In) <= 1 || c.matcher.By == "" {
		return nil
	}

	for i, column := range c.columns {
		if column.Name() == c.matcher.By {
			c.columnIndex = i
			return nil
		}
	}

	return nil
}

func (c *Rows) asKey(val interface{}) interface{} {
	if len(c.matcherColumnDerefs) == 0 {
		rType := reflect.TypeOf(val)
		for rType.Kind() == reflect.Ptr {
			rTypeElem := rType.Elem()
			c.matcherColumnDerefs = append(c.matcherColumnDerefs, xunsafe.NewType(rTypeElem))
			rType = rTypeElem
		}
	}

	for _, deref := range c.matcherColumnDerefs {
		if xunsafe.AsPointer(deref) == nil {
			return nil
		}

		val = deref.Deref(val)
	}

	return io.NormalizeKey(val)
}
