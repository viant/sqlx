package json

import (
	"github.com/stretchr/testify/assert"
	goIo "io"
	"log"
	"testing"
)

type Foo struct {
	ID     int
	Total  float64
	Desc01 string
}

func TestReader_Read(t *testing.T) {
	testCases := []struct {
		description string
		data        func() interface{}
		expected    string
		bufferSizes []int
	}{
		{
			description: "json 001 - ensure Read function properly serves any positive buffer size ",
			data: func() interface{} {

				return []*Foo{
					{
						ID:     1,
						Total:  7.1,
						Desc01: "A1",
					},
					{
						ID:     2,
						Total:  7.2,
						Desc01: "A2",
					},
				}
			},
			expected: `{"ID":1,"Total":7.1,"Desc01":"A1"}
{"ID":2,"Total":7.2,"Desc01":"A2"}
`,
			bufferSizes: []int{1, 30, 68, 128, 512, 1024, 2048, 4096},
		},
	}

	for _, testCase := range testCases {
		testData := testCase.data()

		for _, buffSize := range testCase.bufferSizes {
			reader, err := NewReader(testData)
			assert.Nil(t, err, testCase.description)

			if buffSize < 1 {
				log.Fatalf("Buffer size must be greater than 0 (current: %d)", buffSize)
			}

			buf := make([]byte, buffSize)
			jsonData := make([]byte, 0)
			///	maxIterationCountAllowed := len(testCase.expected)/len(buf) + 2
			maxIterationCountAllowed := 5000
			iterationCounter := 0

			for {
				iterationCounter += 1
				n, err := reader.Read(buf)
				jsonData = append(jsonData, buf[:n]...)

				if err == goIo.EOF {
					break
				} else {
					assert.Nil(t, err, testCase.description, "Current buffer size = ", buffSize)
				}

				if iterationCounter > maxIterationCountAllowed {
					log.Fatalf("Infinite loop danger using buffSize = %d - the maximum count of loop iterations has been exceeded (max: %d, current: %d)", buffSize, maxIterationCountAllowed, iterationCounter)
				}
			}

			var actual = string(jsonData)
			assert.Equal(t, testCase.expected, string(actual), testCase.description, "Buffer size = ", buffSize)
			assert.Nil(t, err, testCase.description)

		}
	}
}
