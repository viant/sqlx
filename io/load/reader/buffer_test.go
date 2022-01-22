package reader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuffer(t *testing.T) {
	testCases := []struct {
		description    string
		initialSize    int
		data           []string
		buffer         []byte
		expectedOffset int
		expectedBuffer string
		expectRead     bool
	}{
		{
			description:    "buffer size equal data size",
			initialSize:    1024,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 12),
			expectedOffset: 12,
			expectedBuffer: "foo name,123",
			expectRead:     true,
		},
		{
			description:    "buffer size lower than data size",
			initialSize:    1,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 12),
			expectedOffset: 12,
			expectedBuffer: "foo name,123",
			expectRead:     true,
		},
		{
			description:    "buffer internal size greater than destination size",
			initialSize:    10,
			data:           []string{"foo name", ",", "123"},
			buffer:         make([]byte, 2),
			expectedOffset: 0,
			expectedBuffer: "\x00\x00",
			expectRead:     false,
		},
	}

	for _, testCase := range testCases[1:] {
		buffer := NewBuffer(testCase.initialSize)
		for _, value := range testCase.data {
			buffer.WriteString(value)
		}

		offset, read := buffer.Read(testCase.buffer)
		assert.Equal(t, testCase.expectedOffset, offset, testCase.description)
		assert.Equal(t, testCase.expectRead, read, testCase.description)
		assert.Equal(t, testCase.expectedBuffer, string(testCase.buffer))
	}
}
