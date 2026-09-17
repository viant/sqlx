package oracle

import (
	"testing"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/registry"
)

func TestOracleMetadataRegistration(t *testing.T) {
	if got := Oracle().Name; got != "Oracle" {
		t.Fatalf("product name: got %q, want Oracle", got)
	}
	registered, ok := registry.Products()["oracle"]
	if !ok || registered.Name != Oracle().Name {
		t.Fatalf("Oracle product is not registered: %#v", registered)
	}

	kinds := []info.Kind{
		info.KindVersion,
		info.KindCatalogs,
		info.KindCatalog,
		info.KindCurrentSchema,
		info.KindSchemas,
		info.KindSchema,
		info.KindTables,
		info.KindTable,
		info.KindSequences,
		info.KindIndexes,
		info.KindIndex,
		info.KindPrimaryKeys,
		info.KindForeignKeys,
		info.KindSession,
		info.KindForeignKeysCheckOn,
		info.KindForeignKeysCheckOff,
	}
	for _, kind := range kinds {
		queries := registry.Lookup(Oracle().Name, kind)
		query := queries.Match(Oracle())
		if query == nil {
			t.Errorf("missing Oracle query for %v", kind)
			continue
		}
		if query.SQL == "" {
			t.Errorf("empty Oracle SQL for %v", kind)
		}
		if err := query.Criteria.Validate(kind); err != nil {
			t.Errorf("invalid Oracle criteria for %v: %v", kind, err)
		}
	}
}

func TestOracleDialectRegistration(t *testing.T) {
	oracleDialect := registry.LookupDialect(Oracle())
	if oracleDialect == nil {
		t.Fatal("Oracle dialect is not registered")
	}
	if !oracleDialect.Transactional {
		t.Error("Oracle dialect should be transactional")
	}
	if oracleDialect.Placeholder != "?" {
		t.Errorf("placeholder: got %q, want ?", oracleDialect.Placeholder)
	}
	if oracleDialect.Insert != dialect.InsertWithSingleValues {
		t.Errorf("insert feature: got %v, want %v", oracleDialect.Insert, dialect.InsertWithSingleValues)
	}
	if oracleDialect.Upsert != dialect.UpsertTypeMergeInto {
		t.Errorf("upsert feature: got %v, want %v", oracleDialect.Upsert, dialect.UpsertTypeMergeInto)
	}
	if oracleDialect.Load != dialect.LoadTypeUnsupported {
		t.Errorf("load feature: got %v, want %v", oracleDialect.Load, dialect.LoadTypeUnsupported)
	}
	if oracleDialect.CanAutoincrement || oracleDialect.CanLastInsertID {
		t.Error("Oracle dialect must not advertise auto-increment or LastInsertID")
	}
	if oracleDialect.MaxPlaceholderCount() != info.DefaultMaxPlaceholders {
		t.Errorf("placeholder limit: got %d, want default %d", oracleDialect.MaxPlaceholderCount(), info.DefaultMaxPlaceholders)
	}
}
