package cache

import (
	"database/sql"
	"github.com/viant/sqlx/io"
	goIo "io"
)

type Entry struct {
	Meta        Meta
	Data        []byte // Entry is used as Iterator, Data is last streamed line.
	Id          string
	WriteCloser *WriteCloser
	ReadCloser  *ReadCloser

	index    int
	RowAdded bool
}

func (e *Entry) Next() bool {
	line, err := ReadLine(e.ReadCloser)
	e.Data = line

	return err == nil
}

func (e *Entry) Has() bool {
	return e.ReadCloser != nil
}

func (e *Entry) SetWriter(writer Writer, closer goIo.Closer) {
	e.WriteCloser = NewWriteCloser(writer, closer)
}

func (e *Entry) SetReader(reader Reader, closer goIo.Closer) {
	e.ReadCloser = NewReadCloser(reader, closer)
}

func (e *Entry) AssignRows(rows *sql.Rows) error {
	if len(e.Meta.Fields) > 0 {
		return nil
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	ioColumns := io.TypesToColumns(types)
	fields, err := ColumnsToFields(ioColumns)
	if err != nil {
		return err
	}

	e.Meta.Fields = fields
	return nil
}

func ColumnsToFields(ioColumns []io.Column) ([]*Field, error) {
	fields := make([]*Field, len(ioColumns))

	for i, column := range ioColumns {
		length, _ := column.Length()
		precision, scale, _ := column.DecimalSize()
		nullable, _ := column.Nullable()
		fields[i] = &Field{
			ColumnName:         column.Name(),
			ColumnLength:       length,
			ColumnPrecision:    precision,
			ColumnScale:        scale,
			ColumnScanType:     column.ScanType().String(),
			_columnScanType:    column.ScanType(),
			ColumnNullable:     nullable,
			ColumnDatabaseName: column.DatabaseTypeName(),
			ColumnTag:          column.Tag(),
		}
	}

	for _, field := range fields {
		if err := field.Init(); err != nil {
			return nil, err
		}
	}

	return fields, nil
}

func (e *Entry) Close() error {
	return notNil(
		e.closeReader(),
		e.flush(),
		e.closeWriter(),
	)
}

func (e *Entry) closeReader() error {
	if e.ReadCloser == nil {
		return nil
	}

	return e.ReadCloser.Close()
}

func (e *Entry) closeWriter() error {
	if e.WriteCloser == nil {
		return nil
	}

	return e.WriteCloser.Close()
}

func (e *Entry) flush() error {
	if e.WriteCloser == nil {
		return nil
	}

	return e.WriteCloser.Flush()
}

func (e *Entry) Write(data []byte) error {
	_, err := e.WriteCloser.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func notNil(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}
