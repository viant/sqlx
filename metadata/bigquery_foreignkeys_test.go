package metadata_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/product/bigquery"
	sqliteproduct "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

func TestServiceInfo_BigQueryForeignKeysShortCircuit(t *testing.T) {
	testCases := []struct {
		name string
		sink func() metadata.Sink
	}{
		{
			name: "preserves_prepopulated_keys",
			sink: func() metadata.Sink {
				return &[]sink.Key{{Name: "existing_fk", Table: "orders", Column: "customer_id"}}
			},
		},
		{
			name: "preserves_prepopulated_string",
			sink: func() metadata.Sink {
				value := "keep-me"
				return &value
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scenario := &blockingMetadataScenario{
				mode:    "success",
				columns: []string{"ignored"},
				values:  []driver.Value{"ignored"},
				started: make(chan struct{}),
			}
			db := openBlockingMetadataDB(t, scenario)
			defer db.Close()

			actual := testCase.sink()
			before := reflect.ValueOf(actual).Elem().Interface()

			meta := metadata.New()
			err := meta.Info(context.Background(), db, info.KindForeignKeys, actual,
				bigquery.BigQuery(),
				option.NewArgs("project", "dataset", "table"))
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}

			after := reflect.ValueOf(actual).Elem().Interface()
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("sink changed: before=%#v after=%#v", before, after)
			}
			if got := scenario.prepareCalls.Load(); got != 0 {
				t.Fatalf("PrepareContext call count = %d, want 0", got)
			}
			if got := scenario.queryContextCalls.Load(); got != 0 {
				t.Fatalf("QueryContext call count = %d, want 0", got)
			}
			if got := scenario.queryCalls.Load(); got != 0 {
				t.Fatalf("Query call count = %d, want 0", got)
			}
		})
	}
}

func TestServiceInfo_BigQueryForeignKeysShortCircuitHonorsCancellation(t *testing.T) {
	scenario := &blockingMetadataScenario{
		mode:    "success",
		columns: []string{"ignored"},
		values:  []driver.Value{"ignored"},
		started: make(chan struct{}),
	}
	db := openBlockingMetadataDB(t, scenario)
	defer db.Close()

	actual := []sink.Key{{Name: "existing_fk"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	meta := metadata.New()
	err := meta.Info(ctx, db, info.KindForeignKeys, &actual,
		bigquery.BigQuery(),
		option.NewArgs("project", "dataset", "table"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Info() error = %v, want %v", err, context.Canceled)
	}
	if got := scenario.prepareCalls.Load(); got != 0 {
		t.Fatalf("PrepareContext call count = %d, want 0", got)
	}
	if got := scenario.queryContextCalls.Load(); got != 0 {
		t.Fatalf("QueryContext call count = %d, want 0", got)
	}
	if got := scenario.queryCalls.Load(); got != 0 {
		t.Fatalf("Query call count = %d, want 0", got)
	}
	if want := []sink.Key{{Name: "existing_fk"}}; !reflect.DeepEqual(actual, want) {
		t.Fatalf("sink changed: got=%#v want=%#v", actual, want)
	}
}

func TestServiceInfo_SQLiteForeignKeysUnaffected(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE artist(artistid INTEGER PRIMARY KEY, artistname TEXT);",
		"CREATE TABLE track(trackid INTEGER, trackname TEXT, trackartist INTEGER, FOREIGN KEY(trackartist) REFERENCES artist(artistid));",
	}
	for _, SQL := range setup {
		if _, err = db.Exec(SQL); err != nil {
			t.Fatalf("Exec(%q) error = %v", SQL, err)
		}
	}

	meta := metadata.New()
	var actual []sink.Key
	err = meta.Info(context.Background(), db, info.KindForeignKeys, &actual,
		sqliteproduct.SQLite3(),
		option.NewArgs("", "", "track"))
	if err != nil {
		t.Fatalf("Info() error = %v", err)
	}

	want := []sink.Key{{
		Name:              "track_artist_fk",
		Type:              "FOREIGN KEY",
		Table:             "track",
		Position:          0,
		Column:            "trackartist",
		ReferenceTable:    "artist",
		ReferenceColumn:   "artistid",
		ConstrainPosition: 0,
		OnUpdate:          "NO ACTION",
		OnDelete:          "NO ACTION",
		OnMatch:           "NONE",
	}}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("foreign keys = %#v, want %#v", actual, want)
	}
}
