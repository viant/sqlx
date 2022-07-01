package cache

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache/ast"
	"reflect"
)

type (
	Field struct {
		ColumnName         string
		ColumnLength       int64
		ColumnPrecision    int64
		ColumnScale        int64
		ColumnScanType     string
		_columnScanType    reflect.Type
		ColumnNullable     bool
		ColumnDatabaseName string
		ColumnTag          *io.Tag
	}
)

func (f *Field) Name() string {
	return f.ColumnName
}

func (f *Field) Length() (length int64, ok bool) {
	return f.ColumnLength, true
}

func (f *Field) DecimalSize() (precision, scale int64, ok bool) {
	return f.ColumnPrecision, f.ColumnScale, true
}

func (f *Field) ScanType() reflect.Type {
	return f._columnScanType
}

func (f *Field) Nullable() (nullable, ok bool) {
	return f.ColumnNullable, true
}

func (f *Field) DatabaseTypeName() string {
	return f.ColumnDatabaseName
}

func (f *Field) Tag() *io.Tag {
	return f.ColumnTag
}

func (f *Field) init() error {
	rType, err := ast.Parse(f.ColumnScanType)
	if err != nil {
		return err
	}

	f._columnScanType = rType
	return nil
}
