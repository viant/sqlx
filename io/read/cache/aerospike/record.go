package aerospike

import as "github.com/aerospike/aerospike-client-go"

type RecordMatched struct {
	key      *as.Key
	record   *as.Record
	keyValue string
	hasKey   bool
}
