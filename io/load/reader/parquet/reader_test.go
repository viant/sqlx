package parquet

import (
	"bytes"
	"encoding/json"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"io"
	goIo "io"
	"log"
	"reflect"
	"strings"
	"testing"
)

type Foo struct {
	ID     int     `parquet:"id,plain,optional"`
	Total  float64 `parquet:"total,plain,optional"`
	Desc01 string  `parquet:"desc_01,plain,optional"`
}

func TestReader_Read(t *testing.T) {
	testCases := []struct {
		description string
		data        func() interface{}
		expected    string
		bufferSizes []int
	}{
		{
			description: "parquet 001 - ensure Read function properly serves any positive buffer size ",
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
			expected:    `{"ID":1,"Total":7.1,"Desc01":"A1"}{"ID":2,"Total":7.2,"Desc01":"A2"}`,
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
			parquetData := make([]byte, 0)
			///	maxIterationCountAllowed := len(testCase.expected)/len(buf) + 2
			maxIterationCountAllowed := 5000
			iterationCounter := 0

			for {
				iterationCounter++
				n, err := reader.Read(buf)
				parquetData = append(parquetData, buf[:n]...)

				if err == goIo.EOF {
					break
				} else {
					assert.Nil(t, err, testCase.description, "Current buffer size = ", buffSize)
				}

				if iterationCounter > maxIterationCountAllowed {
					log.Fatalf("Infinite loop danger using buffSize = %d - the maximum count of loop iterations has been exceeded (max: %d, current: %d)", buffSize, maxIterationCountAllowed, iterationCounter)
				}
			}

			var actual string
			sb := strings.Builder{}
			parquetReader := parquet.NewReader(bytes.NewReader(parquetData))
			elemType := reflect.TypeOf(Foo{})

			for {
				rowPtr := reflect.New(elemType).Interface()
				err := parquetReader.Read(rowPtr)
				if err != nil {
					if err != io.EOF {
						return
					}
					break
				}
				data, err := json.Marshal(rowPtr)
				assert.Nil(t, err, testCase.description)

				sb.WriteString(string(data))
			}

			actual = sb.String()

			assert.Equal(t, testCase.expected, string(actual), testCase.description, "Buffer size = ", buffSize)
			assert.Nil(t, err, testCase.description)

		}
	}
}
