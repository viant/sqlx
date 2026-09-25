package update

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/errx"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/pg"
	"github.com/viant/sqlx/option"
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

func TestUpdateBuildIfMatchNumbersSparsePlaceholders(t *testing.T) {
	type presence struct {
		Title      bool
		Generation bool
		ID         bool
	}
	type record struct {
		Title      string    `sqlx:"title"`
		Generation int       `sqlx:"generation"`
		ID         int       `sqlx:"id,primaryKey=true"`
		Has        *presence `sqlx:"-" setMarker:"true"`
	}
	row := &record{Generation: 8, ID: 1, Has: &presence{Generation: true, ID: true}}
	marker := &option.SetMarker{}
	columns, _, err := io.StructColumnMapper(row, marker)
	if err != nil {
		t.Fatal(err)
	}
	mapped := io.Columns(columns)
	builder, err := NewBuilder("records", mapped.Names(), mapped.PrimaryKeys(), &info.Dialect{PlaceholderResolver: &pg.PlaceholderGenerator{}})
	if err != nil {
		t.Fatal(err)
	}
	got := builder.Build(row, marker, option.IfMatch{Column: "generation", Value: 7})
	want := "UPDATE records SET generation = $1 WHERE id = $2 AND generation = $3"
	if got != want {
		t.Fatalf("SQL=%q, want %q", got, want)
	}
	if got := builder.Build(row, marker, option.IfMatch{Column: "generation OR 1=1", Value: 7}); got != "" {
		t.Fatalf("unmapped match column entered SQL: %q", got)
	}
}

func TestUpdate_NewBuilder_MissingIdentity(t *testing.T) {
	builder, err := NewBuilder("foo", []string{"c1", "c2"}, 0, &info.Dialect{Placeholder: "?"})
	assert.Nil(t, builder)
	assert.True(t, errors.Is(err, errx.ErrMissingIdentity))
	assert.True(t, errx.IsMissingIdentity(err))
}
