// Package sqlfault injects read failures around real database drivers in tests.
package sqlfault

import (
	"context"
	"database/sql/driver"
	"reflect"
)

type Call struct {
	Phase string
	SQL   string
	Row   int
}

// Connector preserves the underlying driver's dialect identity and transaction
// implementation. Before may be called concurrently by partitioned reads.
type Connector struct {
	Base   driver.Driver
	DSN    string
	Before func(context.Context, Call) error
}

func (c *Connector) Driver() driver.Driver { return c.Base }
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.Base.Open(c.DSN)
	if err != nil {
		return nil, err
	}
	return &connection{Conn: conn, owner: c}, nil
}
func (c *Connector) call(ctx context.Context, phase, query string, row int) error {
	if c.Before == nil {
		return nil
	}
	return c.Before(ctx, Call{Phase: phase, SQL: query, Row: row})
}

type connection struct {
	driver.Conn
	owner *Connector
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.owner.call(ctx, "prepare", query, 0); err != nil {
		return nil, err
	}
	var stmt driver.Stmt
	var err error
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = p.PrepareContext(ctx, query)
	} else {
		stmt, err = c.Conn.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &statement{Stmt: stmt, owner: c.owner, query: query}, nil
}
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if tx, ok := c.Conn.(driver.ConnBeginTx); ok {
		return tx.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

type statement struct {
	driver.Stmt
	owner *Connector
	query string
}

func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	values := make([]driver.NamedValue, len(args))
	for i, value := range args {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return s.QueryContext(context.Background(), values)
}
func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.owner.call(ctx, "query", s.query, 0); err != nil {
		return nil, err
	}
	var result driver.Rows
	var err error
	if q, ok := s.Stmt.(driver.StmtQueryContext); ok {
		result, err = q.QueryContext(ctx, args)
	} else {
		values := make([]driver.Value, len(args))
		for i, value := range args {
			values[i] = value.Value
		}
		result, err = s.Stmt.Query(values)
	}
	if err != nil {
		return nil, err
	}
	return &rows{Rows: result, ctx: ctx, owner: s.owner, query: s.query}, nil
}

type rows struct {
	driver.Rows
	ctx   context.Context
	owner *Connector
	query string
	index int
}

func (r *rows) Next(dest []driver.Value) error {
	if err := r.owner.call(r.ctx, "next", r.query, r.index); err != nil {
		return err
	}
	err := r.Rows.Next(dest)
	if err == nil {
		r.index++
	}
	return err
}
func (r *rows) ColumnTypeScanType(i int) reflect.Type {
	if value, ok := r.Rows.(driver.RowsColumnTypeScanType); ok {
		return value.ColumnTypeScanType(i)
	}
	return reflect.TypeOf((*interface{})(nil)).Elem()
}
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	if value, ok := r.Rows.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return value.ColumnTypeDatabaseTypeName(i)
	}
	return ""
}
func (r *rows) ColumnTypeNullable(i int) (bool, bool) {
	if value, ok := r.Rows.(driver.RowsColumnTypeNullable); ok {
		return value.ColumnTypeNullable(i)
	}
	return false, false
}
