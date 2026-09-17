package validator_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/validator"
)

type requiredRow[T any] struct {
	Value T                 `sqlx:"value,required"`
	Has   *requiredPresence `sqlx:"presence=true"`
}
type requiredPresence struct{ Value bool }
type namedNumber int64
type namedFlag bool
type namedPointer *namedNumber

func TestNotNullMatchesSQLiteValues(t *testing.T) {
	zero := namedNumber(0)
	var nullNamed namedPointer
	var nullBool *bool
	tests := []struct {
		name  string
		row   any
		value any
		null  bool
	}{
		{"zero", &requiredRow[int]{}, 0, false},
		{"false", &requiredRow[bool]{}, false, false},
		{"empty string", &requiredRow[string]{}, "", false},
		{"named number", &requiredRow[namedNumber]{}, namedNumber(0), false},
		{"named flag", &requiredRow[namedFlag]{}, namedFlag(false), false},
		{"pointer zero", &requiredRow[namedPointer]{Value: &zero}, namedPointer(&zero), false},
		{"null named pointer", &requiredRow[namedPointer]{}, nullNamed, true},
		{"null bool pointer", &requiredRow[*bool]{}, nullBool, true},
		{"null bytes", &requiredRow[[]byte]{}, []byte(nil), true},
		{"empty bytes", &requiredRow[[]byte]{Value: []byte{}}, []byte{}, false},
		{"invalid nullable string", &requiredRow[sql.NullString]{}, sql.NullString{}, true},
		{"valid empty nullable string", &requiredRow[sql.NullString]{Value: sql.NullString{Valid: true}}, sql.NullString{Valid: true}, false},
		{"invalid nullable bool", &requiredRow[sql.NullBool]{}, sql.NullBool{}, true},
		{"valid false nullable bool", &requiredRow[sql.NullBool]{Value: sql.NullBool{Valid: true}}, sql.NullBool{Valid: true}, false},
		{"zero time", &requiredRow[time.Time]{}, time.Time{}, false},
	}
	service := validator.New()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t, "CREATE TABLE records (value NOT NULL)")
			result, err := service.Validate(context.Background(), h.DB, tc.row, validator.WithShallow(true))
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != tc.null {
				t.Fatalf("validation=%s, want null=%v", result, tc.null)
			}
			_, err = h.DB.Exec("INSERT INTO records(value) VALUES (?)", tc.value)
			if (err != nil) != tc.null {
				t.Fatalf("SQLite error=%v, want null=%v", err, tc.null)
			}
			type count struct {
				N int `sqlx:"n"`
			}
			want := 1
			if tc.null {
				want = 0
			}
			sqlite.AssertRows(t, h, "SELECT COUNT(*) n FROM records", []count{{N: want}})
		})
	}
}

func TestNotNullSparsePresenceAndCache(t *testing.T) {
	for _, sparseFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "full first", true: "sparse first"}[sparseFirst], func(t *testing.T) {
			service := validator.New()
			tests := []struct {
				name           string
				row            *requiredRow[*int]
				sparse, failed bool
			}{
				{"full insert checks omitted", &requiredRow[*int]{Has: &requiredPresence{}}, false, true},
				{"sparse update skips omitted", &requiredRow[*int]{Has: &requiredPresence{}}, true, false},
				{"explicit null update", &requiredRow[*int]{Has: &requiredPresence{Value: true}}, true, true},
				{"unavailable marker checks full", &requiredRow[*int]{}, true, true},
			}
			if sparseFirst {
				tests[0], tests[1] = tests[1], tests[0]
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					var original *requiredPresence
					if tc.row.Has != nil {
						copy := *tc.row.Has
						original = &copy
					}
					opts := []validator.Option{validator.WithShallow(true)}
					if tc.sparse {
						opts = append(opts, validator.WithSetMarker())
					}
					result, err := service.Validate(context.Background(), nil, tc.row, opts...)
					if err != nil {
						t.Fatal(err)
					}
					if result.Failed != tc.failed {
						t.Fatalf("got %s, failed want %v", result, tc.failed)
					}
					if !reflect.DeepEqual(original, tc.row.Has) {
						t.Fatal("validation changed persistence markers")
					}
				})
			}
		})
	}
}

func TestNotNullConcurrentCachedBatch(t *testing.T) {
	service := validator.New()
	zero := 0
	rows := []*requiredRow[*int]{
		{Has: &requiredPresence{}},
		{Has: &requiredPresence{Value: true}},
		{Value: &zero, Has: &requiredPresence{Value: true}},
	}
	var workers sync.WaitGroup
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func(sparse bool) {
			defer workers.Done()
			opts := []validator.Option{validator.WithShallow(true), validator.WithLocation("Items")}
			want := 2
			if sparse {
				opts = append(opts, validator.WithSetMarker())
				want = 1
			}
			result, err := service.Validate(context.Background(), nil, rows, opts...)
			if err != nil {
				t.Error(err)
				return
			}
			if len(result.Violations) != want {
				t.Errorf("sparse=%v: %v", sparse, result)
				return
			}
			if result.Violations[want-1].Location != "Items[1].Value" {
				t.Errorf("wrong batch location: %v", result)
			}
		}(i%2 == 0)
	}
	workers.Wait()
}

type RequiredEmbedded struct {
	Value *int `sqlx:"value,required"`
}
type embeddedRequired struct{ *RequiredEmbedded }
type invalidSQLValue struct{}

var errInvalidSQLValue = errors.New("invalid SQL value")

func (invalidSQLValue) Value() (driver.Value, error) { return nil, errInvalidSQLValue }

func TestNotNullBoundaries(t *testing.T) {
	s := validator.New()
	row := &embeddedRequired{}
	result, err := s.Validate(context.Background(), nil, row, validator.WithShallow(true))
	if err != nil || !result.Failed || row.RequiredEmbedded != nil {
		t.Fatalf("missing embedded holder: result=%v err=%v row=%v", result, err, row)
	}
	_, err = s.Validate(context.Background(), nil, &requiredRow[invalidSQLValue]{}, validator.WithShallow(true))
	if !errors.Is(err, errInvalidSQLValue) {
		t.Fatalf("valuer error lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Validate(ctx, nil, &requiredRow[int]{}, validator.WithShallow(true))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
