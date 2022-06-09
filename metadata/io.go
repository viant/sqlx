package metadata

import (
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/read"
	"reflect"
)

func fetchToString(rows *sql.Rows, dest *string) error {
	if rows.Next() {
		err := rows.Scan(dest)
		if err != nil {
			return err
		}
	}
	err := rows.Scan(dest)

	return nil
}

func fetchToStrings(rows *sql.Rows, dest *[]string) error {
	for rows.Next() {
		item := ""
		if err := rows.Scan(&item); err != nil {
			return err
		}
		*dest = append(*dest, item)
	}
	return nil
}

func fetchStruct(rows *sql.Rows, dest Sink) error {
	valueType := reflect.TypeOf(dest)
	if valueType.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer but had: %T", dest)
	}
	targetValue := reflect.ValueOf(dest)
	switch valueType.Elem().Kind() {
	case reflect.Struct:
		targetType := valueType.Elem()
		reader := read.NewStmt(nil, func() interface{} {
			return reflect.New(targetType).Interface()
		})
		return reader.ReadAll(rows, func(row interface{}) error {
			// TODO: pointer or just a struct
			targetValue.Elem().Set(reflect.ValueOf(row).Elem())
			return nil
		})
	case reflect.Slice:

		targetType := valueType.Elem().Elem()
		isTargetPointer := targetType.Kind() == reflect.Ptr
		reader := read.NewStmt(nil, func() interface{} {
			return reflect.New(targetType).Interface()
		})
		return reader.ReadAll(rows, func(row interface{}) error {
			item := reflect.ValueOf(row)
			if !isTargetPointer {
				item = item.Elem()
			}
			updated := reflect.Append(targetValue.Elem(), item)
			targetValue.Elem().Set(updated)
			return nil
		})
	}
	return nil
}
