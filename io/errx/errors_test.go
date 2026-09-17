package errx

import (
	"errors"
	"testing"
)

func TestIsDuplicateKey(t *testing.T) {
	if !IsDuplicateKey(errors.New("constraint failed: UNIQUE constraint failed: user_oauth_token.user_id, user_oauth_token.provider (1555)")) {
		t.Fatalf("expected duplicate key match")
	}
}

func TestIsConstraint(t *testing.T) {
	if !IsConstraint(errors.New("constraint failed: NOT NULL constraint failed: foo.bar (1299)")) {
		t.Fatalf("expected constraint match")
	}
}

func TestWrappedErrors_Is(t *testing.T) {
	dup := DuplicateKey("insert", "foo", errors.New("duplicate key value violates unique constraint"))
	if !errors.Is(dup, ErrDuplicateKey) {
		t.Fatalf("expected errors.Is(ErrDuplicateKey)")
	}
	missing := MissingIdentity("update", "foo", []string{"c1", "c2"}, 0)
	if !errors.Is(missing, ErrMissingIdentity) {
		t.Fatalf("expected errors.Is(ErrMissingIdentity)")
	}
}
