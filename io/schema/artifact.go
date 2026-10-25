package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
)

// ErrUnsupported identifies a dialect or requested schema feature without a
// faithful native extractor. AllowPartial returns its omissions in the artifact.
var ErrUnsupported = errors.New("schema feature unsupported")

// ExtractOptions selects decoded, literal table names in one schema. A dot in a
// table name is part of that name. Constraints and indexes are always included.
// Dependencies are rejected unless IncludeDependencies is set. Views and
// triggers are opt-in. SQLite includes all views when requested; view/trigger
// dependencies conservatively require all user tables in the selected schema.
// Routines, grants, extensions and data are outside scope.
// Extraction requires a quiescent source; it does not lock concurrent DDL.
type ExtractOptions struct {
	Dialect              *info.Dialect
	Schema               string
	Tables               []string
	IncludeDependencies  bool
	IncludeViews         bool
	IncludeTriggers      bool
	IncludeSequenceState bool
	AllowPartial         bool
}

// Field can describe nested native fields without flattening RECORD/STRUCTs.
// Native statements remain authoritative; extractors need not populate Fields.
type Field struct {
	Name   string
	Type   string
	Mode   string
	Fields []Field `json:",omitempty"`
}

// Object records one extracted object in execution order.
type Object struct {
	Kind   string
	Name   string
	Table  string  `json:",omitempty"`
	Fields []Field `json:",omitempty"`
}

// Omission describes an explicitly incomplete portion of the requested scope.
type Omission struct {
	Object  string
	Feature string
	Reason  string
}

// Artifact is a same-dialect schema, not a migration or a database backup.
// Statements are complete native SQL statements in execution order. Checksum is
// SHA-256 of SQL(), including its final newline. Counters are opt-in and separate
// from identity definitions. Schema remapping is deliberately unsupported.
type Artifact struct {
	Dialect    string
	Driver     string
	Schema     string
	Tables     []string
	Statements []string
	Objects    []Object
	Complete   bool
	Omissions  []Omission
	Checksum   string
}

// SQL renders a deterministic script. Native statements may contain semicolons.
func (a *Artifact) SQL() string {
	if a == nil {
		return ""
	}
	var result strings.Builder
	for _, statement := range a.Statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		result.WriteString(statement)
		// A statement may end in a line comment. Place the terminator on its
		// own line so it cannot become part of that comment.
		tokens, err := lexSQL(statement, a.Dialect)
		terminated := err == nil && len(tokens) > 0 && tokens[len(tokens)-1].word == ";"
		if !terminated {
			result.WriteByte('\n')
			result.WriteByte(';')
		}
		result.WriteByte('\n')
	}
	return result.String()
}

func (a *Artifact) seal() {
	a.Checksum = a.checksum()
	a.Complete = len(a.Omissions) == 0
}

func (a *Artifact) checksum() string {
	digest := sha256.Sum256([]byte(a.SQL()))
	return hex.EncodeToString(digest[:])
}

func schemaDialect(ctx context.Context, db *sql.DB, supplied *info.Dialect, callerTx ...*sql.Tx) (*info.Dialect, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("schema database is required")
	}
	if supplied != nil {
		return supplied, nil
	}
	if len(callerTx) > 0 && callerTx[0] != nil {
		return config.Dialect(ctx, db, callerTx[0])
	}
	return config.Dialect(ctx, db)
}

// Extract returns native DDL for explicitly selected tables. On an unsupported
// feature it returns an incomplete artifact and an error unless AllowPartial.
// Database query errors are always errors, even with AllowPartial.
func Extract(ctx context.Context, db *sql.DB, opts ExtractOptions) (*Artifact, error) {
	d, err := schemaDialect(ctx, db, opts.Dialect)
	if err != nil {
		return nil, err
	}
	if len(opts.Tables) == 0 {
		return nil, fmt.Errorf("schema extraction requires explicit tables")
	}
	for _, name := range opts.Tables {
		if _, err := d.QuoteIdentifier(name); err != nil {
			return nil, err
		}
	}
	artifact := &Artifact{Dialect: d.Name, Driver: fmt.Sprintf("%T", db.Driver()), Schema: opts.Schema}
	switch strings.ToLower(d.Name) {
	case "sqlite":
		err = extractSQLite(ctx, db, d, opts, artifact)
	case "mysql":
		err = extractMySQL(ctx, db, d, opts, artifact)
	case "bigquery":
		err = extractBigQuery(ctx, db, d, opts, artifact)
	case "postgresql":
		err = extractPostgreSQL(ctx, db, d, opts, artifact)
	default:
		artifact.Omissions = append(artifact.Omissions, Omission{Feature: "native schema extraction", Reason: fmt.Sprintf("no faithful extractor for %s", d.Name)})
	}
	if err != nil {
		artifact.Omissions = append(artifact.Omissions, Omission{Feature: "extraction", Reason: err.Error()})
	}
	artifact.seal()
	if err != nil {
		return artifact, err
	}
	if !artifact.Complete && !opts.AllowPartial {
		return artifact, fmt.Errorf("%w: %s", ErrUnsupported, artifact.Omissions[0].Reason)
	}
	return artifact, nil
}
