package schema_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/schema"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestVendorTableRendering(t *testing.T) {
	table := schema.Table{Name: "Jobs", Columns: []io.Column{
		io.NewColumn("ID", "", reflect.TypeOf(""), io.WithTag(&io.Tag{PrimaryKey: true}), io.WithColumnLength(40)),
		io.NewColumn("Created", "", reflect.TypeOf(time.Time{})),
		io.NewColumn("Elapsed", "", reflect.TypeOf(int64(0))),
		io.NewColumn("Count", "", reflect.TypeOf(uint32(0))),
		io.NewColumn("Ratio", "", reflect.TypeOf(float64(0))),
		io.NewColumn("Details", "", reflect.TypeOf((*string)(nil))),
		io.NewColumn("Disabled", "", reflect.TypeOf((*bool)(nil))),
	}}
	for _, tc := range []struct{ product, dataset, expected string }{
		{"SQLite", "main", "CREATE TABLE IF NOT EXISTS \"main\".\"Jobs\" (\n \"ID\" TEXT NOT NULL,\n \"Created\" DATETIME NOT NULL,\n \"Elapsed\" INTEGER NOT NULL,\n \"Count\" INTEGER NOT NULL,\n \"Ratio\" REAL NOT NULL,\n \"Details\" TEXT,\n \"Disabled\" BOOLEAN,\n PRIMARY KEY (\"ID\")\n)"},
		{"MySQL", "`job-db`", "CREATE TABLE IF NOT EXISTS `job-db`.`Jobs` (\n `ID` VARCHAR(40) NOT NULL,\n `Created` DATETIME(6) NOT NULL,\n `Elapsed` BIGINT NOT NULL,\n `Count` BIGINT NOT NULL,\n `Ratio` DOUBLE NOT NULL,\n `Details` TEXT,\n `Disabled` BOOLEAN,\n PRIMARY KEY (`ID`)\n)"},
		{"PostgreSQL", `"Job.Schema"`, "CREATE TABLE IF NOT EXISTS \"Job.Schema\".\"Jobs\" (\n \"id\" TEXT NOT NULL,\n \"created\" TIMESTAMP WITH TIME ZONE NOT NULL,\n \"elapsed\" BIGINT NOT NULL,\n \"count\" BIGINT NOT NULL,\n \"ratio\" DOUBLE PRECISION NOT NULL,\n \"details\" TEXT,\n \"disabled\" BOOLEAN,\n PRIMARY KEY (\"id\")\n)"},
		{"BigQuery", "`project-id.dataset`", "CREATE TABLE IF NOT EXISTS `project-id.dataset.Jobs` (\n `ID` STRING NOT NULL,\n `Created` TIMESTAMP NOT NULL,\n `Elapsed` INT64 NOT NULL,\n `Count` INT64 NOT NULL,\n `Ratio` FLOAT64 NOT NULL,\n `Details` STRING,\n `Disabled` BOOL\n)"},
	} {
		t.Run(tc.product, func(t *testing.T) {
			table.Dataset = tc.dataset
			got, err := table.CreateSQL(&info.Dialect{Product: database.Product{Name: tc.product}})
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestTableRejectsUnsupportedDefinitions(t *testing.T) {
	dialect := &info.Dialect{Product: database.Product{Name: "SQLite"}}
	for _, columns := range [][]io.Column{nil, {nil}, {io.NewColumn("ID", "", reflect.TypeOf([]string{}))}, {io.NewColumn("ID", "", reflect.TypeOf(0)), io.NewColumn("id", "", reflect.TypeOf(0))}, {io.NewColumn("ID", "", reflect.TypeOf(0), io.WithTag(&io.Tag{Autoincrement: true}))}} {
		_, err := (schema.Table{Name: "jobs", Columns: columns}).CreateSQL(dialect)
		require.Error(t, err)
	}
	_, err := (schema.Table{Name: "jobs", Columns: []io.Column{io.NewColumn("ID", "", reflect.TypeOf(""), io.WithTag(&io.Tag{PrimaryKey: true}))}}).CreateSQL(&info.Dialect{Product: database.Product{Name: "MySQL"}})
	require.ErrorContains(t, err, "bounded length")
}

func TestScalarStorageValueDomains(t *testing.T) {
	for _, tc := range []struct {
		product  string
		unsigned string
		floating string
	}{
		{"SQLite", "", "REAL"}, {"MySQL", "BIGINT UNSIGNED", "DOUBLE"},
		{"PostgreSQL", "NUMERIC(20,0)", "DOUBLE PRECISION"}, {"BigQuery", "NUMERIC(20,0)", "FLOAT64"},
	} {
		t.Run(tc.product, func(t *testing.T) {
			d := &info.Dialect{Product: database.Product{Name: tc.product}}
			for _, value := range []any{uint64(0), uint(0), uintptr(0)} {
				typ := reflect.TypeOf(value)
				got, err := d.StorageType(typ, 0)
				if typ.Bits() == 64 && tc.unsigned == "" {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				if typ.Bits() == 64 {
					require.Equal(t, tc.unsigned, got)
				}
			}
			for _, value := range []any{float32(0), float64(0), (*float64)(nil)} {
				got, err := d.StorageType(reflect.TypeOf(value), 0)
				require.NoError(t, err)
				require.Equal(t, tc.floating, got)
			}
		})
	}
}
