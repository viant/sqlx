package info_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	_ "github.com/viant/sqlx/metadata/product/sqlserver"
	"github.com/viant/sqlx/metadata/registry"
)

func TestDialect_EnsurePlaceholders(t *testing.T) {
	var testCases = []struct {
		dialect     info.Dialect
		description string
		sQL         string
		expect      string
	}{
		{
			description: "original placeholders",
			dialect: info.Dialect{
				Placeholder: "?",
			},
			sQL:    "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
			expect: "SELECT COUNT(1) FROM foo WHERE Kind=? AND Active=? AND year > ? ",
		},
	}

	for _, testCase := range testCases {
		actual := testCase.dialect.EnsurePlaceholders(testCase.sQL)
		assert.Equal(t, testCase.expect, actual, testCase.description)
	}
}

func TestDialectMaxPlaceholderCount(t *testing.T) {
	testCases := []struct {
		description string
		productName string
		expected    int
	}{
		{
			description: "mysql",
			productName: "mysql",
			expected:    65530,
		},
		{
			description: "postgresql",
			productName: "postgresql",
			expected:    65530,
		},
		{
			description: "sqlserver",
			productName: "sqlserver",
			expected:    2095,
		},
		{
			description: "sqlite 3",
			productName: "sqlite",
			expected:    994,
		},
	}

	for _, testCase := range testCases {
		product := registry.Products()[testCase.productName]
		if !assert.NotNil(t, product, testCase.description) {
			continue
		}
		dialect := registry.LookupDialect(product)
		if !assert.NotNil(t, dialect, testCase.description) {
			continue
		}
		assert.Equal(t, testCase.expected, dialect.MaxPlaceholderCount(), testCase.description)
	}
	assert.Equal(t, info.DefaultMaxPlaceholders, (&info.Dialect{}).MaxPlaceholderCount())
	assert.Equal(t, info.DefaultMaxPlaceholders, (*info.Dialect)(nil).MaxPlaceholderCount())
}
