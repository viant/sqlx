package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// Metadata owns read-only physical sequence identity for SQLite. A numeric
// MAX allocation may target a table with no sqlite_sequence entry yet.
type Metadata struct{}

func (n *Metadata) CanUse(options ...interface{}) bool {
	return option.AsOptions(options).SequenceIdentityOnly()
}

func (n *Metadata) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	options := option.AsOptions(opts)
	result, ok := target.(*sink.Sequence)
	if !ok || result == nil || options.SequenceTable() == "" {
		return false, fmt.Errorf("sequence identity requires a physical table and Sequence target")
	}
	var queryer sequenceQueryer = options.Tx()
	if options.Tx() == nil {
		connection, err := db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer connection.Close()
		queryer = connection
	}
	identity, err := n.identity(ctx, queryer, options)
	if err != nil {
		return false, err
	}
	if identity.Name == "" || identity.Schema == "" {
		return false, fmt.Errorf("native SQLite sequence identity is unresolved")
	}
	identity.MaxValue, identity.DataType = math.MaxInt64, "int"
	*result = identity
	return false, nil
}
