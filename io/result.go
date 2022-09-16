package io

import "database/sql"

type QueryResult struct {
	sql.Result
	Rows  int64
	Error error
}

func (r *QueryResult) RowsAffected() (int64, error) {
	return r.Rows, r.Error
}
