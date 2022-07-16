package database

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParse(t *testing.T) {
	var useCases = []struct {
		description string
		input       string
		expect      *Product
	}{
		{
			description: "MySQL version",
			input:       "5.6.14-log",
			expect:      &Product{Name: "", Major: 5, Minor: 6, Release: 14},
		},
		{
			description: "PgSQL version",
			input:       "PostgreSQL 9.3.10 on x86_64-unknown-linux-gnu, compiled by gcc (Ubuntu 4.8.2-19ubuntu1) 4.8.2, 64-bit",
			expect:      &Product{Name: "PostgreSQL", Major: 9, Minor: 3, Release: 10},
		},
		{
			description: "Oracle Version",
			input:       "Oracle Database 11g Express Edition Release 11.2.0.2.0 - 64bit Production",
			expect:      &Product{Name: "Oracle Database 11g Express Edition Release", Major: 11, Minor: 2, Release: 0},
		},
		{
			description: "Vertica Version",
			input:       "Vertica Analytic Database v9.1.0-2",
			expect:      &Product{Name: "Vertica Analytic Database", Major: 9, Minor: 1, Release: 0},
		},
		{
			description: "Vertica Version",
			input:       "Vertica Analytic Database v11.1.1-0",
			expect:      &Product{Name: "Vertica Analytic Database", Major: 11, Minor: 1, Release: 1},
		},
		{
			description: "SQL Server Version",
			input:       "Microsoft SQL Server 2000 - 8.00.760 (Intel X86)",
			expect:      &Product{Name: "Microsoft SQL Server 2000", Major: 8, Minor: 0, Release: 760},
		},
		{
			description: "SQLite Version",
			input:       "SQLite - 3.34.0",
			expect:      &Product{Name: "SQLite", Major: 3, Minor: 34, Release: 0},
		},

		//SQLite
	}

	for _, useCase := range useCases {
		actual, err := Parse([]byte(useCase.input))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}
