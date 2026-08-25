package update

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io/errx"
	"github.com/viant/sqlx/metadata/info"
	"testing"
)

func TestUpdate_Build(t *testing.T) {

	var testCases = []struct {
		description   string
		table         string
		columns       []string
		dialect       *info.Dialect
		pkColumnIndex int
		expect        string
	}{
		{
			description: "updated with all columns",
			table:       "foo",
			columns:     []string{"c1", "cN", "cId"},
			dialect: &info.Dialect{
				Placeholder: "?",
			},
			pkColumnIndex: 2,
			expect:        "UPDATE foo SET c1 = ?, cN = ? WHERE cId = ?",
		},
	}

	for _, testCase := range testCases {
		builder, err := NewBuilder(testCase.table, testCase.columns, testCase.pkColumnIndex, testCase.dialect)
		assert.Nil(t, err, testCase.description)
		actual := builder.Build(nil)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}

}

func TestUpdate_NewBuilder_MissingIdentity(t *testing.T) {
	builder, err := NewBuilder("foo", []string{"c1", "c2"}, 0, &info.Dialect{Placeholder: "?"})
	assert.Nil(t, builder)
	assert.True(t, errors.Is(err, errx.ErrMissingIdentity))
	assert.True(t, errx.IsMissingIdentity(err))
}
