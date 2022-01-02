package generators

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/reader"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

//Default represents generator for default strategy
// TODO: Add order to union
type Default struct {
	dialect *info.Dialect
	db      *sql.DB
	session *sink.Session
}

//NewDefault creates a default generator
func NewDefault(dialect *info.Dialect, db *sql.DB, session *sink.Session) *Default {
	return &Default{
		dialect: dialect,
		db:      db,
		session: session,
	}
}

//Apply generated values to the any
func (d *Default) Apply(ctx context.Context, any interface{}, table string) error {
	recordsFn, err := reader.AnyProvider(any)
	if err != nil {
		return err
	}

	record := recordsFn()
	columns, rowMapper, err := d.prepare(ctx, reflect.TypeOf(record), table)
	if err != nil || len(columns) == 0 {
		return err
	}

	SQL := ""
	defaultValues := make([]string, 0)
	values := make([]interface{}, 0) // +1 - Order By column value
	placeholderGetter := d.dialect.PlaceholderGetter()

	i := 0
	for {
		if i > 0 {
			record = recordsFn()
		}
		if record == nil {
			break
		}
		i++

		ptrs, err := rowMapper(record)
		if err != nil {
			return err
		}
		for j, valuePtr := range ptrs {
			genCol := columns[j]
			values = append(values, sqlNil(valuePtr))
			defaultValues = append(defaultValues, sqlValue(*genCol.Default, genCol.Name, placeholderGetter))
		}
		newRowSelect := "SELECT " + strings.Join(defaultValues, ", ")
		if SQL == "" {
			SQL = newRowSelect
		} else {
			SQL = SQL + " UNION " + newRowSelect
		}
		defaultValues = make([]string, 0)
	}

	if len(SQL) == 0 {
		return nil
	}

	recordsFn, err = reader.AnyProvider(any)
	if err != nil {
		return err
	}
	reader, err := reader.New(ctx, d.db, SQL, func() interface{} {
		return recordsFn()
	})
	if err != nil {
		return err
	}

	err = reader.QueryAll(ctx, func(row interface{}) error {
		return nil
	}, values...)

	return err
}

func (d *Default) prepare(ctx context.Context, rType reflect.Type, table string) ([]sink.Column, reader.RowMapper, error) {
	columns, err := d.loadColumnsInfo(ctx, table)
	if err != nil {
		return nil, nil, err
	}

	ioColumns := make([]io.Column, 0)
	genColumns := make([]sink.Column, 0)
	for i, column := range columns {
		if column.Default == nil || strings.HasPrefix(*column.Default, d.dialect.AutoincrementFunc) {
			continue
		}
		ioColumns = append(ioColumns, io.NewColumn(column.Name, column.Type, nil))
		genColumns = append(genColumns, columns[i])
	}

	queryMapper, err := reader.NewStructMapper(ioColumns, rType.Elem(), option.TagSqlx, nil)

	if err != nil {
		return nil, nil, err
	}

	return genColumns, queryMapper, err
}

func (d *Default) countGeneratedColumns(columns []sink.Column) int {
	counter := 0
	for _, column := range columns {
		if column.Default != nil {
			counter++
		}
	}

	return counter
}

func (d *Default) loadColumnsInfo(ctx context.Context, table string) ([]sink.Column, error) {
	meta := metadata.New()
	session, err := d.ensureSession(ctx, meta)

	if err != nil {
		return nil, err
	}
	tableColumns := make([]sink.Column, 0)
	err = meta.Info(ctx, d.db, info.KindTable, &tableColumns, option.NewArgs(session.Catalog, session.Schema, table))

	return tableColumns, err
}

func (d *Default) ensureSession(ctx context.Context, meta *metadata.Service) (*sink.Session, error) {
	if d.session == nil {
		session := new(sink.Session)
		err := meta.Info(ctx, d.db, info.KindSession, session)
		d.session = session
		return session, err
	}
	return d.session, nil
}
