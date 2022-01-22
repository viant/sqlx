package reader

import (
	"github.com/stretchr/testify/assert"
	goIo "io"
	"testing"
)

func TestReader(t *testing.T) {
	testCases := []struct {
		description  string
		config       *Config
		data         func() interface{}
		chunkSize    int
		expectedRead string
	}{
		{
			description: "ensure values are properly escaped",
			config: &Config{
				FieldSeparator:  `,`,
				ObjectSeparator: `#`,
				EncloseBy:       `'`,
				EscapeBy:        `\`,
				NullValue:       "null",
			},
			data: func() interface{} {
				type Foo struct {
					ID      string
					Comment string
				}

				return []*Foo{
					{
						ID:      `\`,
						Comment: `\,`,
					},
					{
						ID:      `\'`,
						Comment: `\'#`,
					},
				}
			},
			chunkSize:    2,
			expectedRead: `'\\','\\\,'#'\\\'','\\\'\#'`,
		},
	}

	for _, testCase := range testCases {
		any := testCase.data()
		reader, _, err := NewReader(any, testCase.config)
		assert.Nil(t, err, testCase.description)
		all, err := goIo.ReadAll(reader)
		assert.Equal(t, testCase.expectedRead, string(all), testCase.description)
		assert.Nil(t, err, testCase.description)
	}
}
