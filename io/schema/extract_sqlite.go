package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

type nativeObject struct{ kind, name, table, ddl string }

func extractSQLite(ctx context.Context, db *sql.DB, d *info.Dialect, opts ExtractOptions, a *Artifact) error {
	if opts.Schema != "" && opts.Schema != "main" {
		a.Omissions = append(a.Omissions, Omission{Object: opts.Schema, Feature: "schema", Reason: "SQLite extraction currently supports main only; attached-schema remapping is unsupported"})
		return nil
	}
	a.Schema = "main"
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, `SELECT type,name,tbl_name,sql FROM main.sqlite_schema WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\' AND sql IS NOT NULL ORDER BY type,name`)
	if err != nil {
		return err
	}
	var objects []nativeObject
	tables := map[string]nativeObject{}
	for rows.Next() {
		var obj nativeObject
		if err = rows.Scan(&obj.kind, &obj.name, &obj.table, &obj.ddl); err != nil {
			rows.Close()
			return err
		}
		objects = append(objects, obj)
		if obj.kind == "table" {
			tables[obj.name] = obj
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	selected := map[string]bool{}
	for _, name := range opts.Tables {
		if _, ok := tables[name]; !ok {
			return fmt.Errorf("schema table %q does not exist in main", name)
		}
		selected[name] = true
	}
	// Native trigger bodies can touch arbitrary tables; views can chain through
	// other views. Conservatively require the complete table scope in either case.
	extras := false
	hasTriggers, hasViews := false, false
	for _, obj := range objects {
		hasTriggers = hasTriggers || obj.kind == "trigger"
		hasViews = hasViews || obj.kind == "view"
		if opts.IncludeViews && obj.kind == "view" || opts.IncludeTriggers && obj.kind == "trigger" {
			extras = true
		}
	}
	if opts.IncludeTriggers && hasTriggers && hasViews && !opts.IncludeViews {
		if !opts.IncludeDependencies {
			return fmt.Errorf("triggers may depend on views; enable IncludeViews or IncludeDependencies")
		}
		opts.IncludeViews = true
	}
	if extras {
		for name := range tables {
			if !selected[name] {
				if !opts.IncludeDependencies {
					return fmt.Errorf("views/triggers require table %q in the extraction scope; select it or enable IncludeDependencies", name)
				}
				selected[name] = true
			}
		}
	}
	dependencies := map[string][]string{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] {
			return nil
		}
		visited[name] = true
		fkRows, err := conn.QueryContext(ctx, `SELECT DISTINCT "table" FROM pragma_foreign_key_list(?, 'main') ORDER BY "table"`, name)
		if err != nil {
			return err
		}
		var refs []string
		for fkRows.Next() {
			var ref string
			if err = fkRows.Scan(&ref); err != nil {
				fkRows.Close()
				return err
			}
			refs = append(refs, ref)
		}
		if err = fkRows.Err(); err != nil {
			fkRows.Close()
			return err
		}
		if err = fkRows.Close(); err != nil {
			return err
		}
		for _, ref := range refs {
			ref = sqliteTableName(tables, ref)
			if _, ok := tables[ref]; !ok {
				return fmt.Errorf("table %q references missing table %q", name, ref)
			}
			dependencies[name] = append(dependencies[name], ref)
			if !selected[ref] {
				if !opts.IncludeDependencies {
					return fmt.Errorf("table %q requires unselected table %q", name, ref)
				}
				selected[ref] = true
			}
			if err = visit(ref); err != nil {
				return err
			}
		}
		return nil
	}
	for _, name := range sortedNames(selected) {
		if err = visit(name); err != nil {
			return err
		}
	}
	order := dependencyOrder(selected, dependencies)
	exported := map[string]bool{}
	for _, name := range order {
		obj := tables[name]
		tokens, _ := lexSQL(obj.ddl, "sqlite")
		if len(tokens) > 1 && tokens[1].word == "VIRTUAL" {
			a.Omissions = append(a.Omissions, Omission{Object: name, Feature: "virtual table", Reason: "SQLite virtual tables require extension and shadow-table handling"})
			continue
		}
		a.Tables = append(a.Tables, name)
		a.Statements = append(a.Statements, obj.ddl)
		a.Objects = append(a.Objects, Object{Kind: "table", Name: name})
		exported[name] = true
	}
	// Autoindexes have NULL sql and are already represented by table constraints.
	for _, obj := range objects {
		if obj.kind == "index" && exported[obj.table] {
			a.Statements = append(a.Statements, obj.ddl)
			a.Objects = append(a.Objects, Object{Kind: "index", Name: obj.name, Table: obj.table})
		}
	}
	if opts.IncludeViews {
		for _, obj := range objects {
			if obj.kind == "view" {
				quoted, _ := d.QuoteIdentifier(obj.name)
				if err = sqliteExplain(ctx, conn, "SELECT * FROM "+quoted); err != nil {
					return fmt.Errorf("SQLite view %q dependencies: %w", obj.name, err)
				}
				a.Statements = append(a.Statements, obj.ddl)
				a.Objects = append(a.Objects, Object{Kind: "view", Name: obj.name})
				exported[obj.name] = true
			}
		}
	}
	if opts.IncludeTriggers {
		for _, name := range sortedNames(exported) {
			if err = sqliteTriggerDependencies(ctx, conn, d, name); err != nil {
				return fmt.Errorf("SQLite trigger dependencies on %q: %w", name, err)
			}
		}
		for _, obj := range objects {
			if obj.kind == "trigger" && exported[obj.table] {
				a.Statements = append(a.Statements, obj.ddl)
				a.Objects = append(a.Objects, Object{Kind: "trigger", Name: obj.name, Table: obj.table})
			}
		}
	}
	if opts.IncludeSequenceState {
		var count int
		if err = conn.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_schema WHERE name='sqlite_sequence'`).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			for _, name := range a.Tables {
				var seq int64
				err = conn.QueryRowContext(ctx, `SELECT seq FROM main.sqlite_sequence WHERE name=?`, name).Scan(&seq)
				if err == sql.ErrNoRows {
					continue
				}
				if err != nil {
					return err
				}
				literal := "'" + strings.ReplaceAll(name, "'", "''") + "'"
				a.Statements = append(a.Statements, fmt.Sprintf("INSERT INTO sqlite_sequence(name,seq) VALUES (%s,%d)", literal, seq))
				a.Objects = append(a.Objects, Object{Kind: "sequence-state", Name: name, Table: name})
			}
		}
	}
	return nil
}

func sortedNames(names map[string]bool) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// SQLite permits forward FK references, including cycles. Stable DFS orders
// acyclic parents first and emits cyclic components deterministically.
func dependencyOrder(selected map[string]bool, deps map[string][]string) []string {
	var result []string
	seen := map[string]bool{}
	var visit func(string)
	visit = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		for _, ref := range deps[name] {
			if selected[ref] {
				visit(ref)
			}
		}
		result = append(result, name)
	}
	for _, name := range sortedNames(selected) {
		visit(name)
	}
	return result
}

func sqliteTableName(tables map[string]nativeObject, name string) string {
	for candidate := range tables {
		if sqliteFold(candidate) == sqliteFold(name) {
			return candidate
		}
	}
	return name
}

func sqliteFold(name string) string {
	result := []byte(name)
	for i, c := range result {
		if c >= 'A' && c <= 'Z' {
			result[i] += 'a' - 'A'
		}
	}
	return string(result)
}

func sqliteExplain(ctx context.Context, conn *sql.Conn, statement string) error {
	rows, err := conn.QueryContext(ctx, "EXPLAIN "+statement)
	if err != nil {
		return err
	}
	return rows.Close()
}

// Prepare EXPLAIN programs without executing mutations. SQLite expands trigger
// programs at prepare time, which detects missing referenced tables/functions.
func sqliteTriggerDependencies(ctx context.Context, conn *sql.Conn, d *info.Dialect, name string) error {
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_schema WHERE type='trigger' AND tbl_name=?`, name).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	quoted, _ := d.QuoteIdentifier(name)
	// A view may define only one of the INSTEAD OF mutation kinds. Compile only
	// the mutation kinds present on this object, using native trigger headers.
	rows, err := conn.QueryContext(ctx, `SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND tbl_name=? ORDER BY name`, name)
	if err != nil {
		return err
	}
	kinds := map[string]bool{}
	for rows.Next() {
		var ddl string
		if err = rows.Scan(&ddl); err != nil {
			rows.Close()
			return err
		}
		tokens, lexErr := lexSQL(ddl, "sqlite")
		if lexErr != nil {
			rows.Close()
			return lexErr
		}
		for _, token := range tokens {
			if token.word == "ON" {
				break
			}
			if token.word == "INSERT" || token.word == "UPDATE" || token.word == "DELETE" {
				kinds[token.word] = true
			}
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if kinds["INSERT"] {
		if err = sqliteExplain(ctx, conn, "INSERT INTO "+quoted+" DEFAULT VALUES"); err != nil {
			return err
		}
	}
	if kinds["DELETE"] {
		if err = sqliteExplain(ctx, conn, "DELETE FROM "+quoted+" WHERE 0"); err != nil {
			return err
		}
	}
	if kinds["UPDATE"] {
		columns, err := conn.QueryContext(ctx, `SELECT name FROM pragma_table_info(?, 'main') ORDER BY cid`, name)
		if err != nil {
			return err
		}
		var assignments []string
		for columns.Next() {
			var col string
			if err = columns.Scan(&col); err != nil {
				columns.Close()
				return err
			}
			q, _ := d.QuoteIdentifier(col)
			assignments = append(assignments, q+"="+q)
		}
		if err = columns.Err(); err != nil {
			columns.Close()
			return err
		}
		if err = columns.Close(); err != nil {
			return err
		}
		return sqliteExplain(ctx, conn, "UPDATE "+quoted+" SET "+strings.Join(assignments, ",")+" WHERE 0")
	}
	return nil
}
