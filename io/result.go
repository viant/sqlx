package io

import "database/sql"

type QueryResult struct {
	sql.Result
	Rows int64
}

func (r *QueryResult) RowsAffected() (int64, error) {
	return r.Rows, nil
}
