package schema_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/internal/testharness/sqlite"
	"github.com/viant/sqlx/io/schema"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestSchemaSQLiteNativeRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := sqlite.New(t,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE "order.key" ("Second" INTEGER NOT NULL, "First" TEXT NOT NULL DEFAULT 'default;value', PRIMARY KEY("Second","First")) WITHOUT ROWID`,
		`CREATE TABLE "select" ("ID" INTEGER PRIMARY KEY AUTOINCREMENT, "ParentSecond" INTEGER NOT NULL DEFAULT 7, "ParentFirst" TEXT NOT NULL DEFAULT 'default;value', "Amount" DECIMAL(12,4) NOT NULL DEFAULT 12.3456 CHECK("Amount">=0), "optional" TEXT DEFAULT 'omitted', "quoted""name" TEXT, "computed" DECIMAL GENERATED ALWAYS AS ("Amount"*2) STORED, FOREIGN KEY("ParentSecond","ParentFirst") REFERENCES "order.key"("Second","First"))`,
		`CREATE UNIQUE INDEX "Index.with.dot" ON "select"("ParentSecond","ParentFirst") WHERE "Amount">0`,
		`CREATE TABLE audit(value TEXT)`,
		`CREATE VIEW "view.with.dot" AS SELECT "ID","Amount","computed" FROM "select"`,
		`CREATE TRIGGER "trigger.with.dot" AFTER INSERT ON "select" BEGIN INSERT INTO audit(value) VALUES (CASE WHEN NEW."Amount">0 THEN 'positive;amount' ELSE 'zero' END); INSERT INTO audit(value) VALUES ('second;statement'); END`,
	)
	source.DB.SetMaxOpenConns(1)
	artifact, err := schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"select"}, IncludeDependencies: true, IncludeViews: true, IncludeTriggers: true})
	require.NoError(t, err)
	require.True(t, artifact.Complete)
	require.NotEmpty(t, artifact.Checksum)
	require.Equal(t, []string{"audit", "order.key", "select"}, artifact.Tables)
	require.Len(t, artifact.Statements, 6)
	again, err := schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"select"}, IncludeDependencies: true, IncludeViews: true, IncludeTriggers: true})
	require.NoError(t, err)
	require.Equal(t, artifact.Checksum, again.Checksum)
	target := sqlite.New(t, `PRAGMA foreign_keys=ON`)
	target.DB.SetMaxOpenConns(1)
	// Exercise the emitted script reader, including its trigger body, on replay.
	result, err := schema.ApplySQL(ctx, target.DB, artifact.SQL(), schema.ApplyOptions{Atomic: true})
	require.NoError(t, err)
	require.Equal(t, 6, result.Executed)
	require.True(t, result.Atomic)
	roundtrip, err := schema.Extract(ctx, target.DB, schema.ExtractOptions{Tables: []string{"select"}, IncludeDependencies: true, IncludeViews: true, IncludeTriggers: true})
	require.NoError(t, err)
	require.Equal(t, artifact.SQL(), roundtrip.SQL())
	changed := *artifact
	changed.Statements = append([]string(nil), artifact.Statements...)
	changed.Statements[0] += " -- changed"
	_, err = schema.Apply(ctx, target.DB, &changed, schema.ApplyOptions{})
	require.ErrorContains(t, err, "checksum mismatch")
	for _, h := range []*sqlite.Harness{source, target} {
		h.Exec(t, `INSERT INTO "order.key"("Second") VALUES(7)`, `INSERT INTO "select" DEFAULT VALUES`)
		var id int
		var amount, computed float64
		var optional sql.NullString
		require.NoError(t, h.DB.QueryRow(`SELECT "ID","Amount","computed","optional" FROM "view.with.dot" JOIN "select" USING("ID","Amount","computed")`).Scan(&id, &amount, &computed, &optional))
		require.Equal(t, 1, id)
		require.Equal(t, 12.3456, amount)
		require.Equal(t, 24.6912, computed)
		require.Equal(t, "omitted", optional.String)
		var audit int
		require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM audit`).Scan(&audit))
		require.Equal(t, 2, audit)
		// The restored FK, composite index and CHECK must behave, not just render.
		_, err = h.DB.Exec(`INSERT INTO "select"("ParentSecond") VALUES(8)`)
		require.ErrorContains(t, err, "FOREIGN KEY")
		_, err = h.DB.Exec(`INSERT INTO "select" DEFAULT VALUES`)
		require.ErrorContains(t, err, "UNIQUE")
		_, err = h.DB.Exec(`INSERT INTO "select"("Amount") VALUES(-1)`)
		require.ErrorContains(t, err, "CHECK")
		h.Exec(t, `INSERT INTO "select"("ID","Amount","optional") VALUES(0,0,NULL)`)
		require.NoError(t, h.DB.QueryRow(`SELECT "optional" FROM "select" WHERE "ID"=0`).Scan(&optional))
		require.False(t, optional.Valid)
		var key1, key2 int
		require.NoError(t, h.DB.QueryRow(`SELECT pk FROM pragma_table_info('order.key') WHERE name='Second'`).Scan(&key1))
		require.Equal(t, 1, key1)
		require.NoError(t, h.DB.QueryRow(`SELECT pk FROM pragma_table_info('order.key') WHERE name='First'`).Scan(&key2))
		require.Equal(t, 2, key2)
	}
}

func TestSchemaDependencyScopeAndIdentityState(t *testing.T) {
	ctx := context.Background()
	source := sqlite.New(t, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`, `CREATE TABLE child(id INTEGER PRIMARY KEY AUTOINCREMENT,parent INTEGER REFERENCES parent(id))`, `INSERT INTO child(id) VALUES(41)`, `DELETE FROM child`)
	_, err := schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"child"}})
	require.ErrorContains(t, err, "requires unselected table")
	for _, state := range []bool{false, true} {
		artifact, err := schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"child"}, IncludeDependencies: true, IncludeSequenceState: state})
		require.NoError(t, err)
		require.Equal(t, []string{"parent", "child"}, artifact.Tables)
		target := sqlite.New(t)
		_, err = schema.Apply(ctx, target.DB, artifact, schema.ApplyOptions{Atomic: true})
		require.NoError(t, err)
		result, err := target.DB.Exec(`INSERT INTO child DEFAULT VALUES`)
		require.NoError(t, err)
		id, err := result.LastInsertId()
		require.NoError(t, err)
		if state {
			require.EqualValues(t, 42, id)
		} else {
			require.EqualValues(t, 1, id)
		}
	}
	_, err = schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"absent"}})
	require.ErrorContains(t, err, "does not exist")
	_, err = schema.Extract(ctx, source.DB, schema.ExtractOptions{})
	require.ErrorContains(t, err, "explicit tables")
	a, err := schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"parent"}, Schema: "attached"})
	require.ErrorIs(t, err, schema.ErrUnsupported)
	require.False(t, a.Complete)
	a, err = schema.Extract(ctx, source.DB, schema.ExtractOptions{Tables: []string{"parent"}, Schema: "attached", AllowPartial: true})
	require.NoError(t, err)
	require.NotEmpty(t, a.Omissions)
	_, err = schema.Apply(ctx, source.DB, a, schema.ApplyOptions{})
	require.ErrorContains(t, err, "incomplete")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = schema.Extract(canceled, source.DB, schema.ExtractOptions{Tables: []string{"parent"}})
	require.True(t, errors.Is(err, context.Canceled))
	a, err = schema.Extract(ctx, source.DB, schema.ExtractOptions{Dialect: &info.Dialect{Product: database.Product{Name: "ANSI"}}, Tables: []string{"parent"}})
	require.ErrorIs(t, err, schema.ErrUnsupported)
	require.NotEmpty(t, a.Omissions)
}

func TestApplyOwnershipAndFailureAccounting(t *testing.T) {
	ctx := context.Background()
	for _, atomic := range []bool{false, true} {
		h := sqlite.New(t)
		result, err := schema.ApplySQL(ctx, h.DB, `CREATE TABLE kept(id INT);CREATE TABLE kept(id INT);CREATE TABLE never(id INT);`, schema.ApplyOptions{Atomic: atomic})
		require.Error(t, err)
		require.Equal(t, 1, result.Executed)
		require.Equal(t, atomic, result.RolledBack)
		var count int
		require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='kept'`).Scan(&count))
		if atomic {
			require.Zero(t, count)
		} else {
			require.Equal(t, 1, count)
		}
	}
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	tx, err := h.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	result, err := schema.ApplySQL(ctx, h.DB, `CREATE TABLE borrowed(id INT);CREATE TABLE borrowed(id INT)`, schema.ApplyOptions{Tx: tx})
	require.Error(t, err)
	require.True(t, result.CallerTransaction)
	require.False(t, result.RolledBack)
	require.Equal(t, 1, result.Executed)
	_, err = tx.ExecContext(ctx, `INSERT INTO borrowed VALUES(1)`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	var count int
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM borrowed`).Scan(&count))
	require.Equal(t, 1, count)
	_, err = schema.ApplySQL(ctx, h.DB, `CREATE TABLE avoided(id INT); DELIMITER $$`, schema.ApplyOptions{})
	require.ErrorContains(t, err, "directive")
	require.NoError(t, h.DB.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='avoided'`).Scan(&count))
	require.Zero(t, count)
	_, err = schema.ApplySQL(ctx, h.DB, `BEGIN; CREATE TABLE avoided(id INT); COMMIT;`, schema.ApplyOptions{})
	require.ErrorContains(t, err, "transaction control")
	_, err = schema.ApplySQL(ctx, h.DB, `SET autocommit=0`, schema.ApplyOptions{})
	require.ErrorContains(t, err, "transaction control")
	_, err = schema.ApplySQL(ctx, h.DB, `PREPARE TRANSACTION 'owned'`, schema.ApplyOptions{})
	require.ErrorContains(t, err, "transaction control")
	_, err = schema.Apply(ctx, h.DB, &schema.Artifact{Dialect: "mysql", Complete: true}, schema.ApplyOptions{})
	require.ErrorContains(t, err, "cannot be applied")
	_, err = schema.Apply(ctx, h.DB, &schema.Artifact{Complete: true, Statements: []string{`CREATE TABLE x(id INT); CREATE TABLE y(id INT)`}}, schema.ApplyOptions{})
	require.ErrorContains(t, err, "exactly one")
}
