package batcher

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestCollection_Reset_Len(t *testing.T) {

	var useCases = []struct {
		description string
	}{
		{
			description: "Reset Len test",
		},
	}

	for _, testCase := range useCases {
		c := NewCollection(reflect.TypeOf(&entity{}))
		c.Append(&entity{})
		l1 := c.Len()

		c.Reset()
		assert.EqualValues(t, c.Len(), 0, testCase.description)

		c.Append(&entity{})
		l2 := c.Len()

		assert.EqualValues(t, l1, l2, testCase.description)
	}
}
