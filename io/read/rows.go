package read

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
)

type Rows struct {
	rows    *sql.Rows
	columns []io.Column
	xTypes  []*xunsafe.Type
	cache   cache.Cache
	entry   *cache.Entry
}

func (c *Rows) CheckType(ctx context.Context, values []interface{}) (bool, error) {
	return true, nil
}

func NewRows(rows *sql.Rows, cache cache.Cache, entry *cache.Entry) (*Rows, error) {
	readerRows := &Rows{
		rows:  rows,
		cache: cache,
		entry: entry,
	}

	if err := readerRows.init(); err != nil {
		return nil, err
	}

	return readerRows, nil
}

func (c *Rows) ConvertColumns() []io.Column {
	return c.columns
}

func (c *Rows) Scanner() func(args ...interface{}) error {
	return c.rows.Scan
}

func (c *Rows) Err() error {
	return c.rows.Err()
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

func (c *Rows) init() error {
	err := c.initColumns()
	if err != nil {
		return err
	}

	c.initXTypes()

	return nil
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
	return nil
}

func (c *Rows) initXTypes() {
	c.xTypes = make([]*xunsafe.Type, len(c.columns))
	for i, column := range c.columns {
		c.xTypes[i] = xunsafe.NewType(column.ScanType())
	}
}

func (c *Rows) Close(ctx context.Context) error {
	var errors []error
	if c.entry != nil {
		if err := c.cache.Close(ctx, c.entry); err != nil {
			errors = append(errors, err)
		}
	}

	if err := c.rows.Close(); err != nil {
		errors = append(errors, err)
	}

	if len(errors) == 0 {
		return nil
	}

	var errMessage string
	for _, err := range errors {
		errMessage += err.Error()
	}

	return fmt.Errorf(errMessage)
}

func (c *Rows) Next() bool {
	return c.rows.Next()
}
