package insert

import (
	"context"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"reflect"
)

type recordUpdater interface {
	prepare(ctx context.Context, options []option.Option, sess *session, at io.ValueAccessor, count int) ([]option.Option, error)
	updateRecord(ctx context.Context, sess *session, record interface{}, columnValue *interface{}, recordCount int, recValues []interface{}, options []option.Option) error
	columnPosition() int
	getColumn() io.Column
	afterFlush(ctx context.Context, values []interface{}, identities []interface{}, affected int64, lastInsertedID int64) (int64, error)
}

func newRecordUpdater(sess *session, column io.Column, position int) (recordUpdater, bool) {
	scanType := column.ScanType()
	for scanType.Kind() == reflect.Ptr {
		scanType = scanType.Elem()
	}

	switch scanType.Kind() {
	case reflect.String:

	default:
		return newNumericSequencer(sess, column, position), true
	}

	return nil, false
}

func newNumericSequencer(sess *session, column io.Column, position int) *numericSequencer {
	return &numericSequencer{
		session:               sess,
		position:              position,
		column:                column,
		shallPresetIdentities: true,
	}
}
