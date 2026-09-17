package sink_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/sink"
)

func TestTableNullableFieldContract(t *testing.T) {
	tableType := reflect.TypeOf(sink.Table{})
	want := map[string]reflect.Type{
		"Catalog":       reflect.TypeOf(""),
		"Schema":        reflect.TypeOf(""),
		"Name":          reflect.TypeOf(""),
		"Comment":       reflect.TypeOf((*string)(nil)),
		"Type":          reflect.TypeOf((*string)(nil)),
		"AutoIncrement": reflect.TypeOf((*string)(nil)),
		"CreateTime":    reflect.TypeOf((*string)(nil)),
		"UpdateTime":    reflect.TypeOf((*string)(nil)),
		"Rows":          reflect.TypeOf((*int)(nil)),
		"Version":       reflect.TypeOf((*string)(nil)),
		"Engine":        reflect.TypeOf((*string)(nil)),
		"SQL":           reflect.TypeOf((*string)(nil)),
	}
	wantTags := map[string]string{
		"Catalog":       "TABLE_CATALOG",
		"Schema":        "TABLE_SCHEMA",
		"Name":          "TABLE_NAME",
		"Comment":       "TABLE_COMMENT",
		"Type":          "TABLE_TYPE",
		"AutoIncrement": "AUTO_INCREMENT",
		"CreateTime":    "CREATE_TIME",
		"UpdateTime":    "UPDATE_TIME",
		"Rows":          "TABLE_ROWS",
		"Version":       "VERSION",
		"Engine":        "ENGINE",
		"SQL":           "DDL",
	}

	for name, wantType := range want {
		field, ok := tableType.FieldByName(name)
		if !ok {
			t.Fatalf("missing Table.%s", name)
		}
		if field.Type != wantType {
			t.Errorf("Table.%s type: got %v, want %v", name, field.Type, wantType)
		}
		if got := field.Tag.Get("sqlx"); got != wantTags[name] {
			t.Errorf("Table.%s sqlx tag: got %q, want %q", name, got, wantTags[name])
		}
	}
}

func TestTableNullableFieldsSQLMapping(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	const query = `
SELECT 'catalog' AS TABLE_CATALOG, 'schema' AS TABLE_SCHEMA, 'missing' AS TABLE_NAME,
       CAST(NULL AS TEXT) AS TABLE_COMMENT, CAST(NULL AS TEXT) AS TABLE_TYPE,
       CAST(NULL AS TEXT) AS AUTO_INCREMENT, CAST(NULL AS TEXT) AS CREATE_TIME,
       CAST(NULL AS TEXT) AS UPDATE_TIME, CAST(NULL AS INTEGER) AS TABLE_ROWS,
       CAST(NULL AS TEXT) AS VERSION, CAST(NULL AS TEXT) AS ENGINE, CAST(NULL AS TEXT) AS DDL
UNION ALL
SELECT 'catalog', 'schema', 'zero', 'comment', 'BASE TABLE', 'NO',
       '2026-08-26', '2026-08-27', 0, '1', 'engine', 'CREATE TABLE zero(id INT)'
UNION ALL
SELECT 'catalog', 'schema', 'positive', 'comment', 'VIEW', 'NO',
       '2026-08-26', '2026-08-27', 7, '2', 'engine', 'CREATE VIEW positive AS SELECT 1'`

	reader, err := read.New(context.Background(), db, query, func() interface{} {
		return &sink.Table{}
	})
	if err != nil {
		t.Fatal(err)
	}

	var tables []*sink.Table
	err = reader.QueryAll(context.Background(), func(row interface{}) error {
		tables = append(tables, row.(*sink.Table))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 3 {
		t.Fatalf("got %d tables, want 3", len(tables))
	}

	missing := tables[0]
	if missing.Comment != nil || missing.Type != nil || missing.AutoIncrement != nil ||
		missing.CreateTime != nil || missing.UpdateTime != nil || missing.Rows != nil ||
		missing.Version != nil || missing.Engine != nil || missing.SQL != nil {
		t.Fatalf("SQL NULL values were not preserved: %#v", missing)
	}
	if tables[1].Rows == nil || *tables[1].Rows != 0 {
		t.Fatalf("zero row count: got %v, want pointer to 0", tables[1].Rows)
	}
	if tables[2].Rows == nil || *tables[2].Rows != 7 {
		t.Fatalf("positive row count: got %v, want pointer to 7", tables[2].Rows)
	}
	if tables[1].Comment == nil || *tables[1].Comment != "comment" {
		t.Fatalf("non-NULL comment: got %v, want pointer to comment", tables[1].Comment)
	}
}

func TestTableNullableFieldsJSON(t *testing.T) {
	comment := "comment"
	rows := 0
	table := sink.Table{Name: "foo", Comment: &comment, Rows: &rows}

	data, err := json.Marshal(table)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["Comment"] != "comment" {
		t.Errorf("Comment JSON value: got %#v, want %q", decoded["Comment"], comment)
	}
	if decoded["Rows"] != float64(0) {
		t.Errorf("Rows JSON value: got %#v, want 0", decoded["Rows"])
	}
	if decoded["Type"] != nil {
		t.Errorf("nil Type JSON value: got %#v, want null", decoded["Type"])
	}
}
