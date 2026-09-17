package errx

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrMissingIdentity indicates SQL generation/execution cannot proceed because
	// identity (primary key) columns were not detected.
	ErrMissingIdentity = errors.New("missing identity")

	// ErrDuplicateKey indicates an insert/update violated a unique constraint.
	// Note: many drivers return opaque error types; use IsDuplicateKey to detect.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrConstraint indicates a generic constraint violation (FK/CK/NOT NULL/etc).
	ErrConstraint = errors.New("constraint violation")
)

// Error carries structured context while remaining compatible with errors.Is().
type Error struct {
	Kind          error
	Op            string
	Table         string
	Columns       []string
	IdentityIndex int
	Cause         error
}

func (e *Error) Error() string {
	sb := &strings.Builder{}
	sb.WriteString("sqlx")
	if e.Op != "" {
		sb.WriteString(" ")
		sb.WriteString(e.Op)
	}
	sb.WriteString(": ")
	if e.Kind != nil {
		sb.WriteString(e.Kind.Error())
	} else {
		sb.WriteString("error")
	}
	if e.Table != "" {
		sb.WriteString(" table=")
		sb.WriteString(e.Table)
	}
	if e.IdentityIndex != 0 {
		sb.WriteString(fmt.Sprintf(" identityIndex=%d", e.IdentityIndex))
	}
	if len(e.Columns) > 0 {
		sb.WriteString(" columns=[")
		sb.WriteString(strings.Join(e.Columns, ","))
		sb.WriteString("]")
	}
	if e.Cause != nil {
		sb.WriteString(": ")
		sb.WriteString(e.Cause.Error())
	}
	return sb.String()
}

func (e *Error) Unwrap() error { return e.Cause }

func (e *Error) Is(target error) bool {
	if target == nil {
		return false
	}
	if e.Kind != nil && target == e.Kind {
		return true
	}
	if e.Cause != nil {
		return errors.Is(e.Cause, target)
	}
	return false
}

func MissingIdentity(op, table string, columns []string, identityIndex int) error {
	return &Error{
		Kind:          ErrMissingIdentity,
		Op:            op,
		Table:         table,
		Columns:       columns,
		IdentityIndex: identityIndex,
	}
}

func DuplicateKey(op, table string, cause error) error {
	return &Error{
		Kind:  ErrDuplicateKey,
		Op:    op,
		Table: table,
		Cause: cause,
	}
}

func Constraint(op, table string, cause error) error {
	return &Error{
		Kind:  ErrConstraint,
		Op:    op,
		Table: table,
		Cause: cause,
	}
}

func IsMissingIdentity(err error) bool { return errors.Is(err, ErrMissingIdentity) }

func IsDuplicateKey(err error) bool {
	if errors.Is(err, ErrDuplicateKey) {
		return true
	}
	msg := strings.ToLower(errString(err))
	return strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "duplicate entry")
}

func IsConstraint(err error) bool {
	if errors.Is(err, ErrConstraint) {
		return true
	}
	msg := strings.ToLower(errString(err))
	return strings.Contains(msg, "constraint failed") ||
		strings.Contains(msg, "violates") ||
		strings.Contains(msg, "not null constraint") ||
		strings.Contains(msg, "foreign key constraint") ||
		strings.Contains(msg, "check constraint")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
