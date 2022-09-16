package io

import "database/sql"

// QueryResult summarizes an executed SQL command.
// use instead of standard Result when you need omit bug: "0 affected rows"
type QueryResult struct {
	sql.Result
	Rows  int64
	Error error
}

// RowsAffected returns count of affected rows
func (r *QueryResult) RowsAffected() (int64, error) {
	return r.Rows, r.Error
}
