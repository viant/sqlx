package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type reservationAuthority struct {
	sequence      sink.Sequence
	table, column string
	mode          string
}

// ReservationMetadata validates the physical AUTO_INCREMENT column rather than
// inferring an allocator from a product name or an authored primary-key tag.
type ReservationMetadata struct{}

func (*ReservationMetadata) CanUse(opts ...interface{}) bool {
	return option.AsOptions(opts).SequenceIdentityOnly()
}
func (m *ReservationMetadata) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Sequence)
	if !ok || out == nil {
		return false, fmt.Errorf("MySQL sequence metadata requires a Sequence target")
	}
	options := option.AsOptions(opts)
	var q queryer = options.Tx()
	var conn *sql.Conn
	if options.Tx() == nil {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Close()
		q = conn
	}
	authority, err := m.resolve(ctx, q, options)
	if err != nil {
		return false, err
	}
	*out = authority.sequence
	return false, nil
}
func (*ReservationMetadata) resolve(ctx context.Context, q queryer, options option.Options) (reservationAuthority, error) {
	result := reservationAuthority{}
	table, column := options.SequenceTable(), options.SequenceColumn()
	parts, err := sqlparser.TableIdentifierParts(table)
	if err != nil {
		return result, err
	}
	colParts, err := sqlparser.TableIdentifierParts(column)
	if err != nil {
		return result, err
	}
	if len(parts) < 1 || len(parts) > 2 || len(colParts) != 1 {
		return result, fmt.Errorf("MySQL reservation requires a mapped [schema.]table and numeric column")
	}
	var schema sql.NullString
	var lowerCase int
	err = q.QueryRowContext(ctx, "SELECT DATABASE(),@@lower_case_table_names,@@SESSION.auto_increment_increment,@@SESSION.auto_increment_offset,@@SESSION.sql_mode").Scan(&schema, &lowerCase, &result.sequence.IncrementBy, &result.sequence.StartValue, &result.mode)
	if err != nil {
		return result, err
	}
	if len(parts) == 2 {
		schema = sql.NullString{String: parts[0], Valid: true}
	}
	if !schema.Valid || schema.String == "" {
		return result, fmt.Errorf("MySQL sequence table %s has no database authority", table)
	}
	table = parts[len(parts)-1]
	tableMatch := "BINARY c.TABLE_SCHEMA=BINARY ? AND BINARY c.TABLE_NAME=BINARY ?"
	if lowerCase != 0 {
		tableMatch = "LOWER(c.TABLE_SCHEMA)=LOWER(?) AND LOWER(c.TABLE_NAME)=LOWER(?)"
	}
	var kind, columnType, extra, engine string
	err = q.QueryRowContext(ctx, `SELECT c.TABLE_SCHEMA,c.TABLE_NAME,c.COLUMN_NAME,c.DATA_TYPE,c.COLUMN_TYPE,c.EXTRA,t.ENGINE
 FROM information_schema.COLUMNS c JOIN information_schema.TABLES t ON BINARY t.TABLE_SCHEMA=BINARY c.TABLE_SCHEMA AND BINARY t.TABLE_NAME=BINARY c.TABLE_NAME
 WHERE `+tableMatch+` AND LOWER(c.COLUMN_NAME)=LOWER(?)`, schema.String, table, colParts[0]).Scan(
		&result.sequence.Schema, &result.table, &result.column, &kind, &columnType, &extra, &engine)
	if err != nil {
		return result, fmt.Errorf("resolve MySQL sequence column %s.%s: %w", options.SequenceTable(), column, err)
	}
	if engine != "InnoDB" {
		return result, fmt.Errorf("MySQL reservation requires an InnoDB source table, found %s for %s.%s", engine, result.sequence.Schema, result.table)
	}
	if !strings.Contains(strings.ToLower(extra), "auto_increment") {
		return result, fmt.Errorf("MySQL column %s.%s.%s is not AUTO_INCREMENT", result.sequence.Schema, result.table, result.column)
	}
	bits := 0
	switch strings.ToLower(kind) {
	case "tinyint":
		bits = 8
	case "smallint":
		bits = 16
	case "mediumint":
		bits = 24
	case "int", "integer":
		bits = 32
	case "bigint":
		bits = 64
	default:
		return result, fmt.Errorf("MySQL generated column %s.%s has non-integer type %s", result.table, result.column, kind)
	}
	result.sequence.MaxValue = math.MaxInt64
	if strings.Contains(strings.ToLower(columnType), "unsigned") {
		if bits < 64 {
			result.sequence.MaxValue = int64(uint64(1)<<bits) - 1
		}
	} else if bits < 64 {
		result.sequence.MaxValue = int64(1)<<(bits-1) - 1
	}
	if result.sequence.IncrementBy <= 0 || result.sequence.StartValue <= 0 {
		return result, fmt.Errorf("MySQL auto_increment increment and offset must be positive")
	}
	if result.sequence.StartValue > result.sequence.IncrementBy {
		result.sequence.StartValue = 1
	}
	result.sequence.Name = quoteName(result.table) + "." + quoteName(result.column)
	result.sequence.DataType = columnType
	return result, nil
}
func quoteName(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }
