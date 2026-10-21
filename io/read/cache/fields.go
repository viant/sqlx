package cache

import (
	"fmt"
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

		initialized bool
	}

	ProjectionField struct {
		Name         string   `json:",omitempty"`
		FieldName    string   `json:",omitempty"`
		ColumnName   string   `json:",omitempty"`
		Source       string   `json:",omitempty"`
		DimensionKey string   `json:",omitempty"`
		MeasureKey   string   `json:",omitempty"`
		Lookup       []string `json:",omitempty"`
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

func (f *Field) Init() error {
	if f.initialized {
		return nil
	}

	rType, err := ast.Parse(f.ColumnScanType)
	if err != nil {
		return fmt.Errorf("cache column %q scan type %q: %w", f.ColumnName, f.ColumnScanType, err)
	}

	f._columnScanType = rType
	f.initialized = true
	return nil
}
