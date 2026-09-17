package validator

import (
	"database/sql"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

type (
	CandidatePolicy struct {
		Previous    any
		FieldFilter func(string) bool
		// DeferredFields marks temporarily unavailable INSERT inputs, independently
		// of sparse coverage. A nonnil function requires nil normalized Previous.
		DeferredFields func(string) bool
		// SatisfiedReferences are trusted, exact reference receipts for this insert.
		// They require nil normalized Previous and nil DeferredFields.
		SatisfiedReferences []Reference
	}

	Options struct {
		CheckUnique          bool
		CheckRef             bool
		Location             string
		Shallow              bool
		MaxPlaceholders      int
		SetMarker            *option.SetMarker
		CandidatePolicies    []CandidatePolicy
		candidatePoliciesSet bool
		fieldFilter          func(string) bool
		fieldFilterSet       bool
		Previous             interface{}
		previousSet          bool
		transaction          *sql.Tx
		dialect              *info.Dialect
	}
	Option func(c *Options)
)

// WithFieldFilter supplies explicit validation coverage in canonical Go field
// names. A nonnil filter takes precedence over WithSetMarker; it does not modify
// any marker. Nil leaves the default full/marker-gated behavior unchanged.
func WithFieldFilter(include func(field string) bool) Option {
	return func(c *Options) { c.fieldFilter, c.fieldFilterSet = include, true }
}

// WithCandidatePolicies supplies an aligned policy for each root candidate in a
// shallow batch. Nil Previous means insert; nil FieldFilter means full coverage.
// A typed nil Previous must point to the candidate's struct type. Empty typed
// batches must contain structs or struct pointers; empty []any is also accepted.
func WithCandidatePolicies(policies []CandidatePolicy) Option {
	return func(c *Options) {
		c.CandidatePolicies = policies
		c.candidatePoliciesSet = true
	}
}

// WithTransaction executes constraint reads in the caller's transaction.
// Validation never commits, rolls back, or opens a replacement transaction.
func WithTransaction(tx *sql.Tx) Option {
	return func(c *Options) { c.transaction = tx }
}

// WithPrevious selects exact per-candidate uniqueness checks. Each previous row
// must already be identity-matched to the corresponding candidate by the caller.
// Nil means every candidate is an insert and excludes no stored row. A supplied
// collection must have exactly one entry per candidate; nil entries are inserts.
// Previous rows must have the same concrete row type and fully loaded constraint
// fields; a projected row must be mapped by its caller first. Use WithShallow(true):
// nested rows require independently matched previous rows, not positional reuse.
// This option never infers identity from candidate values or presence markers.
func WithPrevious(previous interface{}) Option {
	return func(c *Options) { c.Previous, c.previousSet = previous, true }
}

func WithSetMarker() Option {
	return func(c *Options) {
		c.SetMarker = &option.SetMarker{}
	}
}

// WithUnique with unique option
func WithUnique(flag bool) Option {
	return func(c *Options) {
		c.CheckUnique = flag
	}
}

// WithRef with ref key option
func WithRef(flag bool) Option {
	return func(c *Options) {
		c.CheckRef = flag
	}
}

// WithLocation creates with location option
func WithLocation(location string) Option {
	return func(c *Options) {
		c.Location = location
	}
}

// WithShallow with shallow option
func WithShallow(flag bool) Option {
	return func(c *Options) {
		c.Shallow = flag
	}
}

// WithMaxPlaceholders sets the maximum placeholder count for validator queries.
func WithMaxPlaceholders(size int) Option {
	return func(c *Options) {
		c.MaxPlaceholders = size
	}
}

func NewOptions() *Options {
	return &Options{
		CheckUnique: true,
		CheckRef:    true,
	}
}
