package io

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestReader_ReadAll(t *testing.T) {
	var useUses = []struct{
		description string
		query string
		newRow func() interface{}
		params []interface{}
		expect []interface{}
	} {

	}


	for _, useCase := range useUses {
		ctx := context.Background()
		var db *sql.DB
		reader, err := NewReader(ctx, db, useCase.query, useCase.newRow)
		assert.Nil(t, err, useUses)
		var actual = make([]interface{}, 0)
		err = reader.ReadAll(ctx, func(row interface{}) error {
			actual = append(actual, row)
			return nil
		})
		assert.Nil(t, err, useUses)

	}


}