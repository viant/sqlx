package read

import (
	"context"
	"errors"
)

// SourceError identifies a terminal database prepare or query-start failure.
// Error retains the original private diagnostic; callers must not expose it
// as safe public text. Stage is a fixed classification, not SQL or driver text.
// A zero value carries no provenance. Instances are issued only by QueryAll.
type SourceError struct {
	stage string
	cause error
}

func (e *SourceError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}
func (e *SourceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}
func (e *SourceError) Stage() string {
	if e == nil {
		return ""
	}
	return e.stage
}

func (r *Reader) noteSourceFailure(stage string, cause error) {
	if r.sourceErrorProvenance {
		r.sourceFailure = &SourceError{stage: stage, cause: cause}
	}
}

func (r *Reader) withSourceFailure(err error) error {
	if r.sourceFailure == nil || err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// The boundary error is a private fmt wrapper. Match its exact identity in
	// the terminal chain; never infer provenance from message or driver type.
	for current, budget := err, 16; current != nil && budget > 0; budget-- {
		if current == r.sourceFailure.cause {
			return &SourceError{stage: r.sourceFailure.stage, cause: err}
		}
		one, ok := current.(interface{ Unwrap() error })
		if !ok {
			break
		}
		current = one.Unwrap()
	}
	return err
}
