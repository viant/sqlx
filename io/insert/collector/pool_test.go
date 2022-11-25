package collector

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type entity struct {
}

func TestPool_Get_Put(t *testing.T) {

	var useCases = []struct {
		description string
		provider    func() interface{}
	}{
		{
			description: "Get and Put test",
			provider: func() interface{} {
				return NewBatch(reflect.TypeOf(&entity{}), 3, 20)
			},
		},
	}
	for _, testCase := range useCases {
		batchPool := newPool(testCase.provider)

		b1 := batchPool.Get()
		batchPool.Put(b1)

		b2 := batchPool.Get()
		batchPool.Put(b2)

		assert.EqualValues(t, b1, b2, testCase.description)
	}
}
