package info_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"testing"
)

func TestTableIdentifierDialectAuthority(t *testing.T) {
	for _, tc := range []struct{ product, table, dataset, want string }{
		{"SQLite", `"a.b"`, "main", `"main"."a.b"`},
		{"SQLite", `"a""b"`, "", `"a""b"`},
		{"PostgreSQL", `"Job.Table"`, `"Job.Schema"`, `"Job.Schema"."Job.Table"`},
		{"MySQL", "`Job``Table`", "`job-db`", "`job-db`.`Job``Table`"},
		{"BigQuery", "`project-id.dataset.jobs`", "", "`project-id.dataset.jobs`"},
		{"BigQuery", "[project-id:dataset.jobs]", "", "`project-id.dataset.jobs`"},
		{"BigQuery", "jobs", "`project-id.dataset`", "`project-id.dataset.jobs`"},
	} {
		d := &info.Dialect{Product: database.Product{Name: tc.product}}
		got, err := d.TableIdentifier(tc.table, tc.dataset)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	for _, tc := range []struct{ product, table, dataset string }{
		{"SQLite", "jobs; DROP TABLE users", ""}, {"SQLite", "jobs", "main;--"}, {"SQLite", "a.b", "main"}, {"SQLite", "a.b.c", ""},
		{"PostgreSQL", "a.b.c", ""}, {"MySQL", "a.b.c", ""}, {"BigQuery", "a.b.c.d", ""}, {"BigQuery", "`a\\`.b`", ""},
		{"BigQuery", "`a\\nb`", ""}, {"SQLite", "\"a\x00b\"", ""}, {"Other", "jobs", ""}, {"SQLite", "", ""},
	} {
		_, err := (&info.Dialect{Product: database.Product{Name: tc.product}}).TableIdentifier(tc.table, tc.dataset)
		require.Error(t, err, "%+v", tc)
	}
}
