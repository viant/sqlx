package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

func extractMySQL(ctx context.Context, db *sql.DB, d *info.Dialect, opts ExtractOptions, a *Artifact) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	namespace := opts.Schema
	if namespace == "" {
		if err = conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&namespace); err != nil {
			return err
		}
	}
	if namespace == "" {
		return fmt.Errorf("MySQL schema is required")
	}
	a.Schema = namespace
	quotedSchema, err := d.QuoteIdentifier(namespace)
	if err != nil {
		return err
	}
	if opts.IncludeViews {
		a.Omissions = append(a.Omissions, Omission{Feature: "views", Reason: "MySQL view/definer dependency extraction is unsupported"})
	}
	if opts.IncludeTriggers {
		a.Omissions = append(a.Omissions, Omission{Feature: "triggers", Reason: "MySQL trigger dependency extraction is unsupported"})
	}
	selected := map[string]bool{}
	for _, name := range opts.Tables {
		selected[name] = true
	}
	definitions := map[string]string{}
	deps := map[string][]string{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] {
			return nil
		}
		visited[name] = true
		var kind string
		if err := conn.QueryRowContext(ctx, `SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, namespace, name).Scan(&kind); err != nil {
			return fmt.Errorf("inspect MySQL table %q: %w", name, err)
		}
		if kind != "BASE TABLE" {
			a.Omissions = append(a.Omissions, Omission{Object: name, Feature: "table type", Reason: fmt.Sprintf("MySQL table type %s is unsupported", kind)})
			return nil
		}
		quoted, err := d.QuoteIdentifier(name)
		if err != nil {
			return err
		}
		var reported, ddl string
		if err = conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quotedSchema+"."+quoted).Scan(&reported, &ddl); err != nil {
			return err
		}
		if !opts.IncludeSequenceState {
			ddl, err = withoutMySQLCounter(ddl)
			if err != nil {
				return err
			}
		}
		definitions[name] = ddl
		rows, err := conn.QueryContext(ctx, `SELECT DISTINCT REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND REFERENCED_TABLE_NAME IS NOT NULL ORDER BY REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME`, namespace, name)
		if err != nil {
			return err
		}
		var refs []string
		for rows.Next() {
			var ns, ref string
			if err = rows.Scan(&ns, &ref); err != nil {
				rows.Close()
				return err
			}
			if ns != namespace {
				rows.Close()
				return fmt.Errorf("MySQL table %q references another schema %q; cross-schema extraction is unsupported", name, ns)
			}
			refs = append(refs, ref)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err = rows.Close(); err != nil {
			return err
		}
		deps[name] = refs
		for _, ref := range refs {
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
	// MySQL validates referenced tables while creating FKs. Cycles require a
	// separately rendered ALTER phase, which native SHOW CREATE alone cannot supply.
	if cycle := dependencyCycle(selected, deps); cycle != "" {
		a.Omissions = append(a.Omissions, Omission{Object: cycle, Feature: "cyclic foreign keys", Reason: "MySQL cyclic foreign keys require a deferred constraint extraction phase"})
		return nil
	}
	for _, name := range dependencyOrder(selected, deps) {
		if ddl, ok := definitions[name]; ok {
			a.Tables = append(a.Tables, name)
			a.Statements = append(a.Statements, ddl)
			a.Objects = append(a.Objects, Object{Kind: "table", Name: name})
		}
	}
	return nil
}

func withoutMySQLCounter(ddl string) (string, error) {
	tokens, err := lexSQL(ddl, "mysql")
	if err != nil {
		return "", err
	}
	depth := 0
	for i, token := range tokens {
		if token.word == "(" {
			depth++
		}
		if token.word == ")" {
			depth--
		}
		if depth == 0 && token.word == "AUTO_INCREMENT" {
			if i+2 >= len(tokens) || tokens[i+1].word != "=" {
				return "", fmt.Errorf("unsupported MySQL AUTO_INCREMENT table option")
			}
			for _, c := range tokens[i+2].word {
				if c < '0' || c > '9' {
					return "", fmt.Errorf("unsupported MySQL AUTO_INCREMENT counter")
				}
			}
			return strings.TrimSpace(ddl[:token.start] + ddl[tokens[i+2].end:]), nil
		}
	}
	return ddl, nil
}

func dependencyCycle(selected map[string]bool, deps map[string][]string) string {
	active, done := map[string]bool{}, map[string]bool{}
	var visit func(string) string
	visit = func(name string) string {
		if active[name] {
			return name
		}
		if done[name] {
			return ""
		}
		active[name] = true
		for _, ref := range deps[name] {
			if ref == name {
				continue
			}
			if cycle := visit(ref); cycle != "" {
				return cycle
			}
		}
		active[name] = false
		done[name] = true
		return ""
	}
	for _, name := range sortedNames(selected) {
		if cycle := visit(name); cycle != "" {
			return cycle
		}
	}
	return ""
}

func extractBigQuery(ctx context.Context, db *sql.DB, d *info.Dialect, opts ExtractOptions, a *Artifact) error {
	// INFORMATION_SCHEMA native ddl preserves nested fields, required/repeated
	// modes, defaults, partitioning, clustering and native table options. A flat
	// COLUMNS reconstruction would lose those definitions.
	if opts.Schema == "" {
		return fmt.Errorf("BigQuery extraction requires an explicit dataset (or project.dataset)")
	}
	namespace, err := d.TableIdentifier(opts.Schema, "")
	if err != nil {
		return err
	}
	// TableIdentifier quotes the complete BigQuery path as a single identifier.
	path := strings.TrimSuffix(namespace, "`") + ".INFORMATION_SCHEMA.TABLES`"
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if opts.IncludeViews {
		a.Omissions = append(a.Omissions, Omission{Feature: "views", Reason: "BigQuery view dependency extraction is unsupported"})
	}
	if opts.IncludeTriggers {
		a.Omissions = append(a.Omissions, Omission{Feature: "triggers", Reason: "BigQuery does not support table triggers"})
	}
	if opts.IncludeSequenceState {
		a.Omissions = append(a.Omissions, Omission{Feature: "sequence state", Reason: "BigQuery sequence state extraction is unsupported"})
	}
	// Native table DDL does not establish the absence of search/vector indexes or
	// external FK dependencies. Always report that limit, so an artifact becomes
	// executable only after the caller explicitly chooses AllowPartial.
	a.Omissions = append(a.Omissions, Omission{Feature: "indexes and foreign-key dependencies", Reason: "BigQuery native TABLES.ddl extraction does not yet certify indexes or foreign-key dependency closure"})
	selected := map[string]bool{}
	for _, name := range opts.Tables {
		selected[name] = true
	}
	for _, name := range sortedNames(selected) {
		var kind string
		var ddl sql.NullString
		if err = conn.QueryRowContext(ctx, "SELECT table_type,ddl FROM "+path+" WHERE table_name = ?", name).Scan(&kind, &ddl); err != nil {
			return fmt.Errorf("inspect BigQuery table %q: %w", name, err)
		}
		if kind != "BASE TABLE" || !ddl.Valid || strings.TrimSpace(ddl.String) == "" {
			a.Omissions = append(a.Omissions, Omission{Object: name, Feature: "native table definition", Reason: fmt.Sprintf("BigQuery table type %s has no supported base-table DDL", kind)})
			continue
		}
		a.Tables = append(a.Tables, name)
		a.Statements = append(a.Statements, ddl.String)
		a.Objects = append(a.Objects, Object{Kind: "table", Name: name})
	}
	return nil
}
