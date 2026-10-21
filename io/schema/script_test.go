package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/schema"
)

func TestSQLScriptLexicalBoundaries(t *testing.T) {
	cases := []struct {
		name, dialect, script string
		count                 int
	}{
		{"sqlite quotes and comments", "SQLite", `-- ignored ;
CREATE TABLE "semi;table"("a" TEXT DEFAULT 'a'';b', "b" TEXT DEFAULT '\'); /* ; */
INSERT INTO [semi;table] VALUES ('x;y','z'); -- end ;`, 2},
		{"sqlite trigger and case", "sqlite", `CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES(CASE NEW.id WHEN 1 THEN ';' ELSE 'x' END); UPDATE log SET value='end;'; END; SELECT 1;`, 2},
		{"postgres dollar quotes", "PostgreSQL", `CREATE FUNCTION f() RETURNS void AS $body$ BEGIN PERFORM ';'; /* body ; */ END; $body$ LANGUAGE plpgsql; DO $$ BEGIN PERFORM 'a;b'; END $$; SELECT 1;`, 3},
		{"postgres nested comment and escape", "PostgreSQL", `/* outer /* inner ; */ outer */ SELECT E'backslash\';semi'; SELECT 'ordinary\';`, 2},
		{"postgres arrays", "PostgreSQL", `SELECT ARRAY[']',';']; SELECT 2;`, 2},
		{"mysql identifier backslash", "MySQL", "SELECT `back\\`; SELECT 2;", 2},
		{"mysql default escapes", "MySQL", `# ignore ;
SELECT 'escaped\';semicolon'; SELECT ` + "`semi;table`" + ` FROM t;`, 2},
		{"bigquery native nested and triple quotes", "BigQuery", `CREATE TABLE ` + "`project.dataset.table`" + `(items ARRAY<STRUCT<value STRING>>) OPTIONS(description='''one's;
triple quoted string'''); SELECT r"""raw;"x""";`, 2},
		{"sqlite END identifier", "SQLite", `CREATE TRIGGER t AFTER INSERT ON x BEGIN INSERT INTO log VALUES(NEW.end); UPDATE log SET value=NEW.end; END; SELECT 1;`, 2},
		{"only comments", "SQLite", `-- comment ;
/* semicolon ; */`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			statements, err := schema.SplitSQL(tc.script, tc.dialect)
			require.NoError(t, err)
			require.Len(t, statements, tc.count)
		})
	}
	for _, tc := range []struct{ dialect, script string }{
		{"SQLite", "SELECT 'unterminated"}, {"SQLite", "SELECT 1; /* unterminated"}, {"PostgreSQL", "DO $tag$ missing end"},
		{"MySQL", "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 1; END $$"}, {"SQLite", "SELECT 1;\nGO\nSELECT 2"},
		{"PostgreSQL", "\\connect other"}, {"PostgreSQL", "COPY t FROM STDIN;"},
		{"MySQL", "/*!40101 SET sql_mode=ANSI */;"},
		{"MySQL", "SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 1;"},
		{"SQLite", "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1;"},
		{"MySQL", "CREATE PROCEDURE p() BEGIN SELECT 1; END;"},
	} {
		_, err := schema.SplitSQL(tc.script, tc.dialect)
		require.ErrorContains(t, err, "SQL script line")
	}
}

func TestArtifactSQLTerminatesAfterLineComments(t *testing.T) {
	a := &schema.Artifact{Dialect: "SQLite", Complete: true, Statements: []string{"CREATE TABLE a(id INT) -- comment;", "CREATE TABLE b(id INT) -- comment"}}
	statements, err := schema.SplitSQL(a.SQL(), "SQLite")
	require.NoError(t, err)
	require.Len(t, statements, 2)
}
