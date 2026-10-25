package bigquery

import (
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
	"strings"
	"testing"
)

func TestColumnsQuoteQualifiedNamespace(t *testing.T) {
	query := registry.Lookup(BigQuery().Name, info.KindTable).Match(BigQuery())
	if query == nil {
		t.Fatal("missing column metadata query")
	}
	for _, project := range []string{"plainproject", "viant-1p-connect"} {
		sql := strings.NewReplacer("$Args[0]", project, "$Args[1]", "api_data").Replace(query.SQL)
		want := "FROM `" + project + ".api_data.INFORMATION_SCHEMA.COLUMNS`"
		if !strings.Contains(sql, want) {
			t.Fatalf("qualified metadata identifier is not quoted: %s", sql)
		}
	}
	if len(query.Criteria) != 3 || query.Criteria[1].Column != "TABLE_SCHEMA" || query.Criteria[2].Column != "TABLE_NAME" {
		t.Fatal("schema/table restrictions must remain bound criteria")
	}
}
