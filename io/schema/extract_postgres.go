package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

type pgColumn struct {
	name, typ, defaultSQL, identity, generated, collation, typeSchema, compression string
	notNull, customStorage                                                         bool
}
type pgSequence struct {
	column, schema, name, ownership, typ, persistence string
	start, increment, min, max, cache                 int64
	cycle                                             bool
}
type pgConstraint struct{ name, kind, definition string }
type pgTable struct {
	name, qualified, persistence string
	oid                          int64
	columns                      []pgColumn
	sequences                    []pgSequence
	constraints                  []pgConstraint
	indexes                      []nativeObject
	supported                    bool
}

// PostgreSQL catalogs expose native expressions and constraint/index definitions,
// but no SHOW CREATE TABLE. This renderer deliberately handles ordinary tables
// and built-in types; it does not infer DDL for extensions or user-defined types.
// attcompression requires PostgreSQL 14 or newer.
func extractPostgreSQL(ctx context.Context, db *sql.DB, d *info.Dialect, opts ExtractOptions, a *Artifact) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Registry dialect versions describe capability baselines, not necessarily
	// the connected server version. Inspect the server before using its catalogs.
	var serverVersion int
	if err = tx.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&serverVersion); err != nil {
		return err
	}
	if serverVersion < 140000 {
		a.Omissions = append(a.Omissions, Omission{Feature: "server version", Reason: "PostgreSQL schema extraction requires PostgreSQL 14 or newer"})
		return nil
	}
	namespace := opts.Schema
	if namespace == "" {
		if err = tx.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&namespace); err != nil {
			return err
		}
	}
	if namespace == "" {
		return fmt.Errorf("PostgreSQL schema is required")
	}
	a.Schema = namespace
	// Native deparsers qualify non-catalog identifiers with this search_path.
	// SET LOCAL is reverted by our read-only transaction's rollback.
	if _, err = tx.ExecContext(ctx, `SET LOCAL search_path = pg_catalog`); err != nil {
		return err
	}
	qSchema, err := d.QuoteIdentifier(namespace)
	if err != nil {
		return err
	}
	if opts.IncludeViews {
		a.Omissions = append(a.Omissions, Omission{Feature: "views", Reason: "PostgreSQL view dependency extraction is unsupported"})
	}
	if opts.IncludeTriggers {
		a.Omissions = append(a.Omissions, Omission{Feature: "triggers", Reason: "PostgreSQL trigger/function dependency extraction is unsupported"})
	}
	selected := map[string]bool{}
	for _, name := range opts.Tables {
		selected[name] = true
	}
	tables := map[string]*pgTable{}
	dependencies := map[string][]string{}
	var visit func(string) error
	visit = func(name string) error {
		if tables[name] != nil {
			return nil
		}
		quoted, err := d.QuoteIdentifier(name)
		if err != nil {
			return err
		}
		table := &pgTable{name: name, qualified: qSchema + "." + quoted, supported: true}
		tables[name] = table
		var kind, options, access string
		var partition, inherited, security, tablespace, replica bool
		err = tx.QueryRowContext(ctx, `SELECT c.oid::bigint,c.relkind::text,c.relpersistence::text,c.relispartition,(EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) OR EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext WHERE ext.classid='pg_catalog.pg_class'::pg_catalog.regclass AND ext.objid=c.oid AND ext.deptype='e')),c.relrowsecurity OR c.relforcerowsecurity,COALESCE(array_to_string(c.reloptions,','),''),(c.reltablespace<>0 OR EXISTS(SELECT 1 FROM pg_catalog.pg_index ix JOIN pg_catalog.pg_class ic ON ic.oid=ix.indexrelid WHERE ix.indrelid=c.oid AND (ic.reltablespace<>0 OR ic.reloptions IS NOT NULL))),COALESCE(am.amname,''),c.relreplident<>'d' FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE n.nspname=$1 AND c.relname=$2`, namespace, name).Scan(&table.oid, &kind, &table.persistence, &partition, &inherited, &security, &options, &tablespace, &access, &replica)
		if err != nil {
			return fmt.Errorf("inspect PostgreSQL table %q: %w", name, err)
		}
		unsupported := func(feature, reason string) {
			table.supported = false
			a.Omissions = append(a.Omissions, Omission{Object: name, Feature: feature, Reason: reason})
		}
		if kind != "r" || partition || inherited || security || options != "" || tablespace || replica || access != "heap" || (table.persistence != "p" && table.persistence != "u") {
			unsupported("table features", "PostgreSQL extraction supports ordinary heap tables without extension ownership, inheritance, partitioning, row security, storage options, custom tablespaces or replica identity")
			return nil
		}
		rows, err := tx.QueryContext(ctx, `SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,COALESCE(pg_catalog.pg_get_expr(ad.adbin,ad.adrelid),''),a.attidentity::text,a.attgenerated::text,CASE WHEN a.attcollation<>t.typcollation THEN pg_catalog.quote_ident(cn.nspname)||'.'||pg_catalog.quote_ident(coll.collname) ELSE '' END,CASE WHEN EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext WHERE ext.classid='pg_catalog.pg_type'::pg_catalog.regclass AND ext.objid=t.oid AND ext.deptype='e') THEN 'extension' ELSE tn.nspname END,a.attstorage<>t.typstorage,a.attcompression::text FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=a.attcollation LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=coll.collnamespace WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, table.oid)
		if err != nil {
			return err
		}
		for rows.Next() {
			var col pgColumn
			if err = rows.Scan(&col.name, &col.typ, &col.notNull, &col.defaultSQL, &col.identity, &col.generated, &col.collation, &col.typeSchema, &col.customStorage, &col.compression); err != nil {
				rows.Close()
				return err
			}
			table.columns = append(table.columns, col)
			if col.typeSchema != "pg_catalog" || col.customStorage || col.compression != "" || col.collation != "" {
				unsupported("column features", "PostgreSQL custom types, explicit collations, column storage and compression require additional object extraction")
			}
			if col.identity != "" && col.identity != "a" && col.identity != "d" || col.generated != "" && col.generated != "s" && col.generated != "v" {
				unsupported("column generation", "unknown PostgreSQL identity or generated-column mode")
			}
		}
		if err = closeSchemaRows(rows); err != nil {
			return err
		}
		rows, err = tx.QueryContext(ctx, `SELECT a.attname,n.nspname,s.relname,d.deptype::text,pg_catalog.format_type(q.seqtypid,NULL),s.relpersistence::text,q.seqstart,q.seqincrement,q.seqmin,q.seqmax,q.seqcache,q.seqcycle FROM pg_catalog.pg_class s JOIN pg_catalog.pg_namespace n ON n.oid=s.relnamespace JOIN pg_catalog.pg_sequence q ON q.seqrelid=s.oid JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=s.oid AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass JOIN pg_catalog.pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid WHERE d.refobjid=$1 AND d.deptype IN ('a','i') ORDER BY a.attnum,s.relname`, table.oid)
		if err != nil {
			return err
		}
		for rows.Next() {
			var seq pgSequence
			if err = rows.Scan(&seq.column, &seq.schema, &seq.name, &seq.ownership, &seq.typ, &seq.persistence, &seq.start, &seq.increment, &seq.min, &seq.max, &seq.cache, &seq.cycle); err != nil {
				rows.Close()
				return err
			}
			table.sequences = append(table.sequences, seq)
			if seq.schema != namespace || (seq.persistence != "p" && seq.persistence != "u") {
				unsupported("sequence scope", "PostgreSQL owned sequences outside the selected schema or temporary sequences are unsupported")
			}
		}
		if err = closeSchemaRows(rows); err != nil {
			return err
		}
		// Unowned/shared sequences and user-defined expression dependencies are not
		// silently assumed to exist on the target.
		rows, err = tx.QueryContext(ctx, `SELECT DISTINCT n.nspname,s.relname FROM pg_catalog.pg_attrdef ad JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_attrdef'::pg_catalog.regclass AND d.objid=ad.oid AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass JOIN pg_catalog.pg_class s ON s.oid=d.refobjid AND s.relkind='S' JOIN pg_catalog.pg_namespace n ON n.oid=s.relnamespace WHERE ad.adrelid=$1 ORDER BY n.nspname,s.relname`, table.oid)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ns, seqName string
			if err = rows.Scan(&ns, &seqName); err != nil {
				rows.Close()
				return err
			}
			found := false
			for _, seq := range table.sequences {
				found = found || seq.schema == ns && seq.name == seqName
			}
			if !found {
				unsupported("sequence dependencies", "PostgreSQL shared or unowned sequences require an explicit sequence extraction scope")
			}
		}
		if err = closeSchemaRows(rows); err != nil {
			return err
		}
		var external bool
		err = tx.QueryRowContext(ctx, `WITH objects AS (SELECT 'pg_catalog.pg_attrdef'::pg_catalog.regclass AS classid,oid FROM pg_catalog.pg_attrdef WHERE adrelid=$1 UNION ALL SELECT 'pg_catalog.pg_constraint'::pg_catalog.regclass,oid FROM pg_catalog.pg_constraint WHERE conrelid=$1 UNION ALL SELECT 'pg_catalog.pg_class'::pg_catalog.regclass,indexrelid FROM pg_catalog.pg_index WHERE indrelid=$1) SELECT EXISTS(SELECT 1 FROM objects o JOIN pg_catalog.pg_depend dep ON dep.classid=o.classid AND dep.objid=o.oid LEFT JOIN pg_catalog.pg_proc p ON dep.refclassid='pg_catalog.pg_proc'::pg_catalog.regclass AND p.oid=dep.refobjid LEFT JOIN pg_catalog.pg_operator op ON dep.refclassid='pg_catalog.pg_operator'::pg_catalog.regclass AND op.oid=dep.refobjid LEFT JOIN pg_catalog.pg_opclass opc ON dep.refclassid='pg_catalog.pg_opclass'::pg_catalog.regclass AND opc.oid=dep.refobjid LEFT JOIN pg_catalog.pg_collation coll ON dep.refclassid='pg_catalog.pg_collation'::pg_catalog.regclass AND coll.oid=dep.refobjid LEFT JOIN pg_catalog.pg_type typ ON dep.refclassid='pg_catalog.pg_type'::pg_catalog.regclass AND typ.oid=dep.refobjid JOIN pg_catalog.pg_namespace n ON n.oid=COALESCE(p.pronamespace,op.oprnamespace,opc.opcnamespace,coll.collnamespace,typ.typnamespace) WHERE n.nspname<>'pg_catalog' OR EXISTS(SELECT 1 FROM pg_catalog.pg_depend ext WHERE ext.classid=dep.refclassid AND ext.objid=dep.refobjid AND ext.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass))`, table.oid).Scan(&external)
		if err != nil {
			return err
		}
		if external {
			unsupported("expression dependencies", "PostgreSQL expressions using non-catalog functions, operators, collations or index operator classes require additional object extraction")
		}
		rows, err = tx.QueryContext(ctx, `SELECT c.conname,c.contype::text,pg_catalog.pg_get_constraintdef(c.oid,true),COALESCE(n.nspname,''),COALESCE(r.relname,'') FROM pg_catalog.pg_constraint c LEFT JOIN pg_catalog.pg_class r ON r.oid=c.confrelid LEFT JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace WHERE c.conrelid=$1 ORDER BY c.conname`, table.oid)
		if err != nil {
			return err
		}
		var refs []string
		for rows.Next() {
			var constraint pgConstraint
			var ns, ref string
			if err = rows.Scan(&constraint.name, &constraint.kind, &constraint.definition, &ns, &ref); err != nil {
				rows.Close()
				return err
			}
			switch constraint.kind {
			case "p", "u", "c", "f", "x":
				table.constraints = append(table.constraints, constraint)
			default:
				unsupported("constraint", "unknown PostgreSQL constraint kind "+constraint.kind)
			}
			if constraint.kind == "f" {
				if ns != namespace {
					unsupported("foreign-key scope", "PostgreSQL cross-schema foreign keys are unsupported")
				} else {
					refs = append(refs, ref)
				}
			}
		}
		if err = closeSchemaRows(rows); err != nil {
			return err
		}
		dependencies[name] = refs
		rows, err = tx.QueryContext(ctx, `SELECT ci.relname,pg_catalog.pg_get_indexdef(i.indexrelid,0,true),i.indisvalid AND i.indisready,am.amname FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ci ON ci.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=ci.relam WHERE i.indrelid=$1 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conindid=i.indexrelid AND c.conrelid=i.indrelid) ORDER BY ci.relname`, table.oid)
		if err != nil {
			return err
		}
		for rows.Next() {
			var obj nativeObject
			var valid bool
			var method string
			if err = rows.Scan(&obj.name, &obj.ddl, &valid, &method); err != nil {
				rows.Close()
				return err
			}
			if !valid || !pgBuiltinIndex(method) {
				unsupported("index", "PostgreSQL invalid indexes or extension index access methods are unsupported")
				continue
			}
			obj.kind = "index"
			obj.table = name
			table.indexes = append(table.indexes, obj)
		}
		if err = closeSchemaRows(rows); err != nil {
			return err
		}
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
	order := dependencyOrder(selected, dependencies)
	// Serial sequences must precede table defaults. Identity sequences are created
	// by their column definition and retain their native sequence names/options.
	for _, name := range order {
		table := tables[name]
		if !table.supported {
			continue
		}
		for _, seq := range table.sequences {
			if seq.ownership == "i" {
				continue
			}
			qualified := pgQualified(d, seq.schema, seq.name)
			prefix := "CREATE SEQUENCE "
			if seq.persistence == "u" {
				prefix = "CREATE UNLOGGED SEQUENCE "
			}
			a.Statements = append(a.Statements, prefix+qualified+" AS "+seq.typ+" "+pgSequenceOptions(seq))
			a.Objects = append(a.Objects, Object{Kind: "sequence", Name: seq.name, Table: name})
		}
	}
	for _, name := range order {
		table := tables[name]
		if !table.supported {
			continue
		}
		var definitions []string
		for _, col := range table.columns {
			q, _ := d.QuoteIdentifier(col.name)
			definition := q + " " + col.typ
			if col.identity != "" {
				mode := "ALWAYS"
				if col.identity == "d" {
					mode = "BY DEFAULT"
				}
				var identity *pgSequence
				for i := range table.sequences {
					seq := &table.sequences[i]
					if seq.column == col.name && seq.ownership == "i" {
						identity = seq
						break
					}
				}
				if identity == nil {
					return fmt.Errorf("PostgreSQL identity column %s.%s has no owned sequence", name, col.name)
				}
				definition += " GENERATED " + mode + " AS IDENTITY (SEQUENCE NAME " + pgQualified(d, identity.schema, identity.name) + " " + pgSequenceOptions(*identity) + ")"
			} else if col.generated != "" {
				mode := "STORED"
				if col.generated == "v" {
					mode = "VIRTUAL"
				}
				definition += " GENERATED ALWAYS AS (" + col.defaultSQL + ") " + mode
			} else if col.defaultSQL != "" {
				definition += " DEFAULT " + col.defaultSQL
			}
			if col.notNull {
				definition += " NOT NULL"
			}
			definitions = append(definitions, definition)
		}
		for _, constraint := range table.constraints {
			if constraint.kind == "f" {
				continue
			}
			q, _ := d.QuoteIdentifier(constraint.name)
			definitions = append(definitions, "CONSTRAINT "+q+" "+constraint.definition)
		}
		prefix := "CREATE TABLE "
		if table.persistence == "u" {
			prefix = "CREATE UNLOGGED TABLE "
		}
		a.Statements = append(a.Statements, prefix+table.qualified+" (\n "+strings.Join(definitions, ",\n ")+"\n)")
		a.Tables = append(a.Tables, name)
		a.Objects = append(a.Objects, Object{Kind: "table", Name: name})
	}
	for _, name := range order {
		table := tables[name]
		if !table.supported {
			continue
		}
		for _, constraint := range table.constraints {
			if constraint.kind != "f" {
				continue
			}
			q, _ := d.QuoteIdentifier(constraint.name)
			a.Statements = append(a.Statements, "ALTER TABLE "+table.qualified+" ADD CONSTRAINT "+q+" "+constraint.definition)
			a.Objects = append(a.Objects, Object{Kind: "foreign-key", Name: constraint.name, Table: name})
		}
		for _, obj := range table.indexes {
			a.Statements = append(a.Statements, obj.ddl)
			a.Objects = append(a.Objects, Object{Kind: "index", Name: obj.name, Table: name})
		}
		for _, seq := range table.sequences {
			qualified := pgQualified(d, seq.schema, seq.name)
			if seq.ownership != "i" {
				q, _ := d.QuoteIdentifier(seq.column)
				a.Statements = append(a.Statements, "ALTER SEQUENCE "+qualified+" OWNED BY "+table.qualified+"."+q)
				a.Objects = append(a.Objects, Object{Kind: "sequence-owner", Name: seq.name, Table: name})
			}
			if opts.IncludeSequenceState {
				var value int64
				var called bool
				if err = tx.QueryRowContext(ctx, "SELECT last_value,is_called FROM "+qualified).Scan(&value, &called); err != nil {
					return err
				}
				literal := "'" + strings.ReplaceAll(qualified, "'", "''") + "'"
				a.Statements = append(a.Statements, fmt.Sprintf("SELECT pg_catalog.setval(%s::pg_catalog.regclass,%d,%t)", literal, value, called))
				a.Objects = append(a.Objects, Object{Kind: "sequence-state", Name: seq.name, Table: name})
			}
		}
	}
	return nil
}

func closeSchemaRows(rows *sql.Rows) error {
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	return rows.Close()
}
func pgQualified(d *info.Dialect, schema, name string) string {
	ns, _ := d.QuoteIdentifier(schema)
	obj, _ := d.QuoteIdentifier(name)
	return ns + "." + obj
}
func pgSequenceOptions(seq pgSequence) string {
	cycle := "NO CYCLE"
	if seq.cycle {
		cycle = "CYCLE"
	}
	return fmt.Sprintf("START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d CACHE %d %s", seq.start, seq.increment, seq.min, seq.max, seq.cache, cycle)
}
func pgBuiltinIndex(method string) bool {
	switch method {
	case "btree", "hash", "gist", "spgist", "gin", "brin":
		return true
	}
	return false
}
