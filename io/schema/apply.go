package schema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

// ApplyOptions controls DDL execution. A supplied Tx remains caller-owned and is
// never committed or rolled back. Atomic requests an owned DDL transaction and
// cannot be combined with Tx. SQLite and PostgreSQL support this option; MySQL
// and BigQuery DDL do not. Without either option all statements share one pinned
// connection and successful statements remain applied on a later failure.
// PostgreSQL sequence state and side effects of authored functions retain the
// server's nontransactional behavior even inside a transaction.
type ApplyOptions struct {
	Dialect      *info.Dialect
	Tx           *sql.Tx
	Atomic       bool
	AllowPartial bool
}

// ApplyResult accounts for successful Exec calls, including calls subsequently
// rolled back. Executed does not claim that a caller-owned transaction committed.
// Atomic is true when statements ran inside an owned or caller transaction.
type ApplyResult struct {
	Executed          int
	Atomic            bool
	RolledBack        bool
	CallerTransaction bool
}

// Apply executes an artifact as authored, failing on existing objects or other
// SQL errors. It does not infer migrations, disable constraints, or reset tables.
func Apply(ctx context.Context, db *sql.DB, artifact *Artifact, opts ApplyOptions) (*ApplyResult, error) {
	result := &ApplyResult{}
	if artifact == nil {
		return result, fmt.Errorf("schema artifact is required")
	}
	if artifact.Checksum != "" && artifact.Checksum != artifact.checksum() {
		return result, fmt.Errorf("schema artifact checksum mismatch")
	}
	if (!artifact.Complete || len(artifact.Omissions) > 0) && !opts.AllowPartial {
		return result, fmt.Errorf("incomplete schema artifact requires AllowPartial")
	}
	d, err := schemaDialect(ctx, db, opts.Dialect, opts.Tx)
	if err != nil {
		return result, err
	}
	if artifact.Dialect != "" && !strings.EqualFold(artifact.Dialect, d.Name) {
		return result, fmt.Errorf("schema dialect %s cannot be applied to %s", artifact.Dialect, d.Name)
	}
	if opts.Tx != nil && opts.Atomic {
		return result, fmt.Errorf("Atomic and caller Tx are mutually exclusive")
	}
	ddlTransactions := strings.EqualFold(d.Name, "sqlite") || strings.EqualFold(d.Name, "postgresql")
	if (opts.Atomic || opts.Tx != nil) && !ddlTransactions {
		return result, fmt.Errorf("%w: %s does not support transactional schema application", ErrUnsupported, d.Name)
	}
	// Validate the entire input before executing anything. Explicit arrays still
	// reject transaction ownership/client directives and contain one statement each.
	for index, statement := range artifact.Statements {
		parts, err := SplitSQL(statement, d.Name)
		if err != nil {
			return result, fmt.Errorf("schema statement %d: %w", index+1, err)
		}
		if len(parts) != 1 {
			return result, fmt.Errorf("schema statement %d must contain exactly one SQL statement", index+1)
		}
		if transactionControl(parts[0], d.Name) {
			return result, fmt.Errorf("schema statement %d: transaction control requires ApplyOptions", index+1)
		}
	}
	var executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}
	var owned *sql.Tx
	if opts.Tx != nil {
		executor = opts.Tx
		result.Atomic = true
		result.CallerTransaction = true
	} else {
		conn, err := db.Conn(ctx)
		if err != nil {
			return result, err
		}
		defer conn.Close()
		if opts.Atomic {
			owned, err = conn.BeginTx(ctx, nil)
			if err != nil {
				return result, err
			}
			defer owned.Rollback()
			executor = owned
			result.Atomic = true
		} else {
			executor = conn
		}
	}
	for index, statement := range artifact.Statements {
		if _, err := executor.ExecContext(ctx, statement); err != nil {
			if owned != nil {
				result.RolledBack = owned.Rollback() == nil
			}
			return result, fmt.Errorf("schema statement %d (%d executed): %w", index+1, result.Executed, err)
		}
		result.Executed++
	}
	if owned != nil {
		if err := owned.Commit(); err != nil {
			return result, fmt.Errorf("commit schema: %w", err)
		}
	}
	return result, nil
}

// ApplySQL parses the entire script before executing its ordered statements.
func ApplySQL(ctx context.Context, db *sql.DB, script string, opts ApplyOptions) (*ApplyResult, error) {
	d, err := schemaDialect(ctx, db, opts.Dialect, opts.Tx)
	if err != nil {
		return &ApplyResult{}, err
	}
	statements, err := SplitSQL(script, d.Name)
	if err != nil {
		return &ApplyResult{}, err
	}
	opts.Dialect = d
	return Apply(ctx, db, &Artifact{Dialect: d.Name, Statements: statements, Complete: true}, opts)
}

func isTransactionWord(word string) bool {
	switch word {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "START", "END", "ABORT":
		return true
	}
	return false
}

func transactionControl(statement, dialect string) bool {
	tokens, _ := lexSQL(statement, dialect)
	if len(tokens) == 0 {
		return false
	}
	if isTransactionWord(tokens[0].word) {
		return true
	}
	if len(tokens) > 1 && tokens[0].word == "PREPARE" && tokens[1].word == "TRANSACTION" {
		return true
	}
	if tokens[0].word == "SET" {
		for _, token := range tokens[1:] {
			if token.word == "AUTOCOMMIT" || token.word == "TRANSACTION" {
				return true
			}
		}
	}
	return false
}
