package io

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestReader_ReadAll(t *testing.T) {
	var useCases = []struct{
		description string
		query string
		newRow func() interface{}
		params []interface{}
		expect []interface{}
		initSQL []string
	} {

	}


	outer: for _, useCase := range useCases {
		ctx := context.Background()
		var db *sql.DB

		for _, SQL := range useCase.initSQL {
			_, err := db.Exec(SQL)
			if ! assert.Nil(t, err, useCase.description) {
				continue outer
			}
		}

		reader, err := NewReader(ctx, db, useCase.query, useCase.newRow)
		assert.Nil(t, err, useCases)
		var actual = make([]interface{}, 0)
		err = reader.ReadAll(ctx, func(row interface{}) error {
			actual = append(actual, row)
			return nil
		})
		assert.Nil(t, err, useCases)

	}


}