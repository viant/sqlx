package cache

import (
	"database/sql"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/viant/sqlx/io/read/cache/ast"
)

func TestSQLNullableScanTypeMetadataRoundTrip(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[sql.NullString](), reflect.TypeFor[sql.NullInt64](), reflect.TypeFor[sql.Null[uint64]](), reflect.TypeFor[sql.Null[[]byte]](), reflect.TypeFor[json.RawMessage]()} {
		field := &Field{ColumnName: "value", ColumnScanType: typ.String(), ColumnNullable: true}
		if err := field.Init(); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(field)
		if err != nil {
			t.Fatal(err)
		}
		var restored Field
		if err = json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		if err = restored.Init(); err != nil {
			t.Fatal(err)
		}
		if restored.ScanType() != typ {
			t.Fatalf("type changed: %s -> %s", typ, restored.ScanType())
		}
	}
	field := &Field{ColumnScanType: "sql.Unknown"}
	if field.Init() == nil || field.Init() == nil {
		t.Fatal("failed parsing must not become initialized")
	}
	if _, err := ast.Parse("sql.Null[unknown]"); err == nil {
		t.Fatal("unknown generic argument accepted")
	}
}

func TestSQLJSONScanMetadataAndReplay(t *testing.T) {
	for _, projected := range []bool{false, true} {
		var target json.RawMessage
		holder := &ScanTypeHolder{}
		holder.InitType([]any{&target})
		field := &Field{ColumnName: "conditions", ColumnScanType: "json.RawMessage"}
		if err := field.Init(); err != nil {
			t.Fatal(err)
		}
		entry := &Entry{Meta: Meta{Fields: []*Field{field}}}
		if projected {
			entry.Meta.ProjectedIndexes = []int{0}
		}
		if !holder.Match(entry) {
			t.Fatal(holder.Mismatch(entry))
		}
		scan := NewScanner(holder, nil).New(entry)
		if projected {
			scan = NewProjectedScanner(entry, []int{0}, holder, nil)
		}
		for _, value := range []string{`["feature"]`, `{"id":18446744073709551615}`, `[]`, `null`} {
			entry.Data = []byte("[" + value + "]")
			if err := scan(&target); err != nil {
				t.Fatal(err)
			}
			if value == "null" {
				if target != nil {
					t.Fatal("NULL left stale JSON")
				}
			} else if string(target) != value {
				t.Fatalf("JSON changed: %s -> %s", value, target)
			}
		}
	}
	var text string
	holder := &ScanTypeHolder{}
	holder.InitType([]any{&text})
	f := &Field{ColumnScanType: "json.RawMessage"}
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}
	entry := &Entry{Meta: Meta{Fields: []*Field{f}}}
	if !holder.Match(entry) {
		t.Fatal("JSON driver metadata rejected string scan")
	}
	entry.Meta.Type = []string{"json.RawMessage"}
	if holder.Match(entry) {
		t.Fatal("raw cached JSON treated as escaped string")
	}
}

func TestSQLNullableUint64CacheReplay(t *testing.T) {
	for _, projected := range []bool{false, true} {
		var target sql.Null[uint64]
		holder := &ScanTypeHolder{}
		holder.InitType([]any{&target})
		field := &Field{ColumnName: "value", ColumnScanType: reflect.TypeFor[sql.Null[uint64]]().String(), ColumnNullable: true}
		if err := field.Init(); err != nil {
			t.Fatal(err)
		}
		entry := &Entry{Meta: Meta{Fields: []*Field{field}}}
		if projected {
			entry.Meta.ProjectedIndexes = []int{0}
		}
		if !holder.Match(entry) {
			t.Fatalf("driver nullable type rejected: %+v", holder.Mismatch(entry))
		}
		scan := NewScanner(holder, nil).New(entry)
		if projected {
			scan = NewProjectedScanner(entry, []int{0}, holder, nil)
		}
		for _, want := range []sql.Null[uint64]{{V: 0, Valid: true}, {V: math.MaxUint64, Valid: true}, {}, {V: 1 << 63, Valid: true}, {}} {
			var err error
			entry.Data, err = json.Marshal([]any{want})
			if err != nil {
				t.Fatal(err)
			}
			if err = scan(&target); err != nil {
				t.Fatal(err)
			}
			if target != want {
				t.Fatalf("lossy replay: %+v -> %+v", want, target)
			}
		}
		for _, data := range []string{`[18446744073709551615]`, `[0]`, `[null]`} {
			entry.Data = []byte(data)
			if err := scan(&target); err != nil {
				t.Fatal(err)
			}
			switch data {
			case `[18446744073709551615]`:
				if target.V != math.MaxUint64 || !target.Valid {
					t.Fatal(target)
				}
			case `[0]`:
				if target.V != 0 || !target.Valid {
					t.Fatal(target)
				}
			case `[null]`:
				if target.Valid || target.V != 0 {
					t.Fatal("NULL left stale value", target)
				}
			}
		}
	}
}

func TestSQLNullableCacheRepresentationCompatibility(t *testing.T) {
	var scalar uint64
	holder := &ScanTypeHolder{}
	holder.InitType([]any{&scalar})
	f := &Field{ColumnScanType: "sql.Null[uint64]"}
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}
	entry := &Entry{Meta: Meta{Fields: []*Field{f}}}
	if !holder.Match(entry) {
		t.Fatal("driver metadata should describe scalar destination")
	}
	entry.Meta.Type = []string{"sql.Null[uint64]"}
	if holder.Match(entry) {
		t.Fatal("stored nullable objects must not match scalar decoder")
	}
}
