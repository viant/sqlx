package insert

import (
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

func TestBuilderExplicitIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name, product, table, identity, want string
		columns                              []string
		escape                               byte
	}{
		{
			name: "ANSI fallback backticks", product: "ANSI", table: "`session/attributes`",
			columns: []string{"value", "user_id", "name"},
			want:    "INSERT INTO `session/attributes`(value,user_id,name) VALUES (?,?,?)",
		},
		{
			name: "ANSI fallback double quotes", product: "ansi", table: `"session/attributes"`,
			columns: []string{"value", "user_id", "name"},
			want:    `INSERT INTO "session/attributes"(value,user_id,name) VALUES (?,?,?)`,
		},
		{
			name: "unknown product", product: "Other", table: "`a.b`",
			columns: []string{"select", "Mixed Case"},
			want:    "INSERT INTO `a.b`(select,Mixed Case) VALUES (?,?)",
		},
		{
			name: "empty product legacy wrapper", table: "`a.b`", escape: '`',
			columns: []string{"select", "Mixed Case"},
			want:    "INSERT INTO ``a.b``(select,Mixed Case) VALUES (?,?)",
		},
		{
			name: "SQLite dotted name", product: "SQLite", table: `"a.b"`,
			columns: []string{"select", "Mixed Case"},
			want:    `INSERT INTO "a.b"("select","Mixed Case") VALUES (?,?)`,
		},
		{
			name: "SQLite escaped quotes", product: "sQlItE", table: `"a""b"`,
			columns: []string{`Mixed"Case`},
			want:    `INSERT INTO "a""b"("Mixed""Case") VALUES (?)`,
		},
		{
			name: "MySQL exact columns", product: "MySQL", table: "`MixedTable`",
			columns: []string{"select", "Mixed Case"},
			want:    "INSERT INTO `MixedTable`(`select`,`Mixed Case`) VALUES (?,?)",
		},
		{
			name: "MySQL unquoted columns", product: "mysql", table: "MixedTable",
			columns: []string{"MixedCase"},
			want:    "INSERT INTO `MixedTable`(MixedCase) VALUES (?)",
		},
		{
			name: "PostgreSQL exact columns and identity", product: "PostgreSQL", table: `"Job.Schema"."Job.Table"`, identity: "MixedID",
			columns: []string{"select", "MixedCase"},
			want:    `INSERT INTO "Job.Schema"."Job.Table"("select","MixedCase") VALUES (?,?) RETURNING "MixedID"`,
		},
		{
			name: "BigQuery path", product: "BigQuery", table: "`project-id.dataset.jobs`",
			columns: []string{"select", "MixedCase"},
			want:    "INSERT INTO `project-id.dataset.jobs`(`select`,`MixedCase`) VALUES (?,?)",
		},
		{
			name: "ANSI fallback identity", product: "ANSI", table: "`session/attributes`", identity: "MixedID",
			columns: []string{"MixedCase"},
			want:    "INSERT INTO `session/attributes`(MixedCase) VALUES (?) RETURNING MixedID",
		},
		{
			name: "unknown product identity", product: "Other", table: `"a.b"`, identity: "MixedID",
			columns: []string{"MixedCase"},
			want:    `INSERT INTO "a.b"(MixedCase) VALUES (?) RETURNING MixedID`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &info.Dialect{
				Product: database.Product{Name: tc.product}, Placeholder: "?",
				SpecialKeywordEscapeQuote: tc.escape, CanReturning: tc.identity != "",
			}
			b, err := NewBuilder(tc.table, tc.columns, d, tc.identity, 1)
			if err != nil {
				t.Fatal(err)
			}
			if actual := b.Build(nil, option.BatchSize(1)); actual != tc.want {
				t.Fatalf("SQL = %q, want %q", actual, tc.want)
			}
		})
	}
	for _, tc := range []struct{ product, table string }{
		{"MySQL", "jobs; DROP TABLE users"},
		{"MySQL", "`unterminated"},
		{"SQLite", `"unterminated`},
		{"PostgreSQL", `"a"."b"."c"`},
		{"BigQuery", "`a.b.c.d`"},
	} {
		t.Run("invalid "+tc.product+" "+tc.table, func(t *testing.T) {
			d := &info.Dialect{Product: database.Product{Name: tc.product}, Placeholder: "?"}
			if _, err := NewBuilder(tc.table, []string{"value"}, d, "", 1); err == nil {
				t.Fatal("malformed concrete-dialect identifier accepted")
			}
		})
	}
}
