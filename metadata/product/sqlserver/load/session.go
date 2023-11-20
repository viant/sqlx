package load

import (
	"context"
	"database/sql"
	"encoding/json"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"github.com/viant/xunsafe"
)

// Session represents session
type Session struct {
	*io.Transaction
	dialect *info.Dialect
}

// NewSession returns new session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

// Exec inserts given data to database using "LOAD DATA LOCAL INFILE"
// note: local_infile=1 must be enabled on database
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error) {
	loadHint := option.Options(options).LoadHint()

	var bulkOptions = mssql.BulkOptions{}
	if loadHint != "" {
		if err := json.Unmarshal([]byte(loadHint), &bulkOptions); err != nil {
			return nil, err
		}
	}

	valueAt, dataSize, err := io.Values(data)
	if err != nil {
		return nil, err
	}

	dataType := io.EnsureDereference(valueAt(0))

	tableColumns, err := s.getMetaColumns(db, tableName)
	if err != nil {
		return nil, err
	}

	err = s.begin(ctx, db, options)
	if err != nil {
		return nil, err
	}

	matcher := io.NewMatcher(nil)
	matchResult, err := matcher.Match(dataType, tableColumns)
	if err != nil && !io.IsMatchedError(err) {
		return nil, err
	}

	var matched = make([]io.Field, 0)
	for _, v := range matchResult {
		if v.MatchesType == true {
			matched = append(matched, v)
		}
	}

	SQL := mssql.CopyIn(tableName, bulkOptions, io.Fields(matched).ColumnNames()...)
	stmt, err := s.Transaction.Prepare(SQL)
	defer stmt.Close()

	xStruct := &xunsafe.Struct{Fields: io.Fields(matched).XFields()}

	// TODO Possibly add batching mechanism
	for i := 0; i < dataSize; i++ {
		args := asArgs(xStruct, valueAt(i))
		_, err := stmt.Exec(args...)
		if err != nil {
			return nil, err
		}
	}

	res, err := stmt.Exec()
	if err != nil {
		return nil, err
	}

	err = s.end(err)
	if err != nil {
		return nil, err
	}

	err = stmt.Close()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Session) getMetaColumns(db *sql.DB, tableName string) ([]io.Column, error) {
	meta := metadata.New()

	options := option.Options{s.dialect}

	resSchema := sink.Schema{}
	err := meta.Info(context.TODO(), db, info.KindCurrentSchema, &resSchema, options...)
	if err != nil {
		return nil, err
	}

	options = append(options, option.NewArgs(resSchema.Catalog, resSchema.Name, tableName))

	resColumn := []sink.Column{}
	err = meta.Info(context.TODO(), db, info.KindTable, &resColumn, options...)
	if err != nil {
		return nil, err
	}

	var columns = make([]io.Column, len(resColumn))
	for i, v := range resColumn {
		columns[i] = io.NewColumn(v.Name, v.Type, nil)
	}
	return columns, nil
}

func asArgs(xStruct *xunsafe.Struct, record interface{}) []interface{} {
	var args = make([]interface{}, len(xStruct.Fields))
	ptr := xunsafe.AsPointer(record)

	for i := range xStruct.Fields {
		field := &xStruct.Fields[i]
		args[i] = field.Interface(ptr)
		//fmt.Printf("%v %T %v\n", field.Name, args[i], args[i])
	}
	return args
}

func (s *Session) begin(ctx context.Context, db *sql.DB, options []option.Option) error {
	var err error
	s.Transaction, err = io.TransactionFor(ctx, s.dialect, db, options)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) end(err error) error {
	if s.Tx == nil {
		return err
	}

	if err != nil {
		return s.Transaction.RollbackWithErr(err)
	}

	return s.Transaction.Commit()
}
