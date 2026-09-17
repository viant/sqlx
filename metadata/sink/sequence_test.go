package sink

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// MySQL has problems with some cases (StartValue > IncrementBy && IncrementBy > 1)
// see test sqlx/metadata/product/mysql/test/sequence_test.go
func TestSequence_NextValue(t *testing.T) {

	var testCases = []struct {
		description string
		seq         *Sequence
		records     int64
		expect      int64
	}{
		{
			description: "1 node active cluster, with local node 1 offset, seqValue: 1, recordCnt: 3",
			records:     3,
			seq: &Sequence{
				Value:       1,
				IncrementBy: 1,
				StartValue:  1, //1 -> 2 -> 3 -> 4
			},
			expect: 4,
		},
		{
			description: "2 nodes active cluster, with local node 3 offset, seqValue: 1, recordCnt: 1",
			records:     1,
			seq: &Sequence{
				Value:       1,
				IncrementBy: 2,
				StartValue:  3, //3 -> 5
			},
			expect: 5,
		},
		{
			description: "2 nodes active cluster, with local node 3 offset, seqValue: 7, recordCnt: 3",
			records:     3,
			seq: &Sequence{
				Value:       7,
				IncrementBy: 2,
				StartValue:  3, //3 -> 5 -> 7 -> 9 -> 11 -> 13
			},
			expect: 13,
		},
		{
			description: "2 nodes active cluster, with local node 3 offset, seqValue: 1, recordCnt: 3",
			records:     3,
			seq: &Sequence{
				Value:       1,
				IncrementBy: 2,
				StartValue:  3, //3 -> 5 -> 7 -> 9
			},
			expect: 9,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 25, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       25,
				IncrementBy: 10,
				StartValue:  5, //5 -> 15 -> 25 -> 35 -> 45
			},
			expect: 45,
		},
		{
			description: "10 nodes active cluster, with local node 15 offset, seqValue: 5, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       5,
				IncrementBy: 10,
				StartValue:  15, //15 -> 25 -> 35
			},
			expect: 35,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 3, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       3,
				IncrementBy: 10,
				StartValue:  5, //5 -> 15 -> 25
			},
			expect: 25,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 13, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       13,
				IncrementBy: 10,
				StartValue:  5, //5 -> 15 -> 25 -> 35
			},
			expect: 35,
		},
	}

	for _, testCase := range testCases {
		actual := testCase.seq.NextValue(testCase.records)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}
}

func TestSequence_MinValue(t *testing.T) {

	var testCases = []struct {
		description string
		seq         *Sequence
		records     int64
		expect      int64
	}{
		{
			description: "1 node active cluster, with local node 1 offset, seqValue: 4, recordCnt: 3",
			records:     3,
			seq: &Sequence{
				Value:       4,
				IncrementBy: 1,
				StartValue:  1, //4 -> 3 -> 2 -> 1
			},
			expect: 1,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 35, recordCnt: 1",
			records:     1,
			seq: &Sequence{
				Value:       35,
				IncrementBy: 10,
				StartValue:  5,
			},
			expect: 25, // 35 -> 25
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 35, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       35,
				IncrementBy: 10,
				StartValue:  5, // 35 -> 25 -> 15
			},
			expect: 15,
		},
		{
			description: "10 nodes active cluster, with local node 15 offset, seqValue: 5, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       5,
				IncrementBy: 10,
				StartValue:  15, //15
			},
			expect: 15,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 3, recordCnt: 1",
			records:     1,
			seq: &Sequence{
				Value:       3,
				IncrementBy: 10,
				StartValue:  5, //5
			},
			expect: 5,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 3, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       3,
				IncrementBy: 10,
				StartValue:  5, //5
			},
			expect: 5,
		},
		{
			description: "10 nodes active cluster, with local node 5 offset, seqValue: 38, recordCnt: 2",
			records:     2,
			seq: &Sequence{
				Value:       38,
				IncrementBy: 10,
				StartValue:  5, //38 -> 25 -> 15
			},
			expect: 15,
		},
		{
			description: "3 nodes active cluster, with local node 10 offset, seqValue: 13, recordCnt: 2",
			records:     1,
			seq: &Sequence{
				Value:       10,
				IncrementBy: 3,
				StartValue:  10, //10 -> 10
			},
			expect: 10,
		},
		// Additional edge cases
		{
			description: "Value equals StartValue, recordCnt:1",
			records:     1,
			seq: &Sequence{
				Value:       5,
				IncrementBy: 10,
				StartValue:  5,
			},
			expect: 5,
		},
		{
			description: "Value equals StartValue, recordCnt:3",
			records:     3,
			seq: &Sequence{
				Value:       5,
				IncrementBy: 10,
				StartValue:  5,
			},
			expect: 5,
		},
		{
			description: "Value less than StartValue, recordCnt:1",
			records:     1,
			seq: &Sequence{
				Value:       7,
				IncrementBy: 3,
				StartValue:  10,
			},
			expect: 10,
		},
		{
			description: "Value less than StartValue, recordCnt:4",
			records:     4,
			seq: &Sequence{
				Value:       7,
				IncrementBy: 3,
				StartValue:  10,
			},
			expect: 10,
		},
		{
			description: "Aligned and > StartValue, recordCnt:1",
			records:     1,
			seq: &Sequence{
				Value:       12,
				IncrementBy: 5,
				StartValue:  2, // 2,7,12 -> expect 7
			},
			expect: 7,
		},
		{
			description: "Aligned and > StartValue, recordCnt:2",
			records:     2,
			seq: &Sequence{
				Value:       12,
				IncrementBy: 5,
				StartValue:  2, // 2,7,12 -> expect 2
			},
			expect: 2,
		},
		{
			description: "Large step, multiple records",
			records:     3,
			seq: &Sequence{
				Value:       250,
				IncrementBy: 50,
				StartValue:  100, // 100,150,200,250 -> expect 100
			},
			expect: 100,
		},
		{
			description: "StartValue > IncrementBy edge, Value==StartValue",
			records:     2,
			seq: &Sequence{
				Value:       7,
				IncrementBy: 3,
				StartValue:  7,
			},
			expect: 7,
		},
		{
			description: "IncrementBy=1 baseline, Value>StartValue",
			records:     4,
			seq: &Sequence{
				Value:       10,
				IncrementBy: 1,
				StartValue:  1, // 10 -> 6
			},
			expect: 6,
		},
	}

	for _, testCase := range testCases {
		actual := testCase.seq.MinValue(testCase.records)
		assert.EqualValues(t, testCase.expect, actual, testCase.description)
	}
}
