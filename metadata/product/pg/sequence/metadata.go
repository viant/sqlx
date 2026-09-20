package sequence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type authority struct {
	sequence sink.Sequence
	oid      int64
	min, max int64
}

// Metadata resolves identity/serial/default and explicit sequence authorities
// from PostgreSQL catalogs. Names and search_path are interpreted by PostgreSQL.
type Metadata struct{}

func (*Metadata) CanUse(opts ...interface{}) bool {
	return option.AsOptions(opts).SequenceIdentityOnly()
}
func (m *Metadata) Handle(ctx context.Context, db *sql.DB, target interface{}, opts ...interface{}) (bool, error) {
	out, ok := target.(*sink.Sequence)
	if !ok || out == nil {
		return false, fmt.Errorf("PostgreSQL sequence metadata requires a Sequence target")
	}
	options := option.AsOptions(opts)
	var q queryer = db
	if tx := options.Tx(); tx != nil {
		q = tx
	}
	result, err := m.resolve(ctx, q, options)
	if err != nil {
		return false, err
	}
	*out = result.sequence
	return false, nil
}

func (*Metadata) resolve(ctx context.Context, q queryer, options option.Options) (authority, error) {
	result := authority{}
	table, column := options.SequenceTable(), options.SequenceColumn()
	if table == "" || column == "" {
		return result, fmt.Errorf("PostgreSQL reservation requires the mapped table and numeric column")
	}
	var tableOID int64
	var actualColumn, kind, identityMode string
	var baseType int
	err := q.QueryRowContext(ctx, `SELECT c.oid, a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod),a.attidentity,
 COALESCE(NULLIF(t.typbasetype,0),a.atttypid)
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
 JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
 WHERE c.oid=pg_catalog.to_regclass($1) AND a.attname=(pg_catalog.parse_ident($2,true))[1]
 AND cardinality(pg_catalog.parse_ident($2,true))=1 AND a.attnum>0 AND NOT a.attisdropped`, table, column).Scan(&tableOID, &actualColumn, &kind, &identityMode, &baseType)
	if err != nil {
		return result, fmt.Errorf("resolve PostgreSQL column %s.%s: %w", table, column, err)
	}
	switch baseType {
	case 20:
		result.min, result.max = math.MinInt64, math.MaxInt64
	case 21:
		result.min, result.max = math.MinInt16, math.MaxInt16
	case 23:
		result.min, result.max = math.MinInt32, math.MaxInt32
	default:
		return result, fmt.Errorf("PostgreSQL sequence column %s.%s has non-integer type %s", table, column, kind)
	}

	explicit := ""
	if args := options.Args(); args != nil {
		values := args.Unwrap()
		if len(values) >= 3 {
			if name, ok := values[2].(string); ok && name != table {
				explicit = name
			}
		}
	}
	var sequenceOID sql.NullInt64
	if explicit != "" {
		err = q.QueryRowContext(ctx, "SELECT pg_catalog.to_regclass($1)::oid::bigint", explicit).Scan(&sequenceOID)
	} else {
		err = q.QueryRowContext(ctx, "SELECT pg_catalog.to_regclass(pg_catalog.pg_get_serial_sequence($1,$2))::oid::bigint", table, actualColumn).Scan(&sequenceOID)
		if err == nil && !sequenceOID.Valid {
			// A nextval default need not own its sequence. Catalog dependencies reveal
			// its actual sequence without parsing COLUMN_DEFAULT or guessing a name.
			rows, e := q.QueryContext(ctx, `SELECT DISTINCT d.refobjid::bigint FROM pg_catalog.pg_attrdef ad
    JOIN pg_catalog.pg_attribute a ON a.attrelid=ad.adrelid AND a.attnum=ad.adnum
    JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_attrdef'::regclass AND d.objid=ad.oid AND d.refclassid='pg_catalog.pg_class'::regclass
    JOIN pg_catalog.pg_class s ON s.oid=d.refobjid AND s.relkind='S'
    WHERE ad.adrelid=$1::oid AND a.attname=$2`, tableOID, actualColumn)
			if e != nil {
				return result, e
			}
			count := 0
			for rows.Next() {
				if e = rows.Scan(&sequenceOID.Int64); e != nil {
					rows.Close()
					return result, e
				}
				count++
			}
			e = rows.Err()
			closeErr := rows.Close()
			if e != nil {
				return result, e
			}
			if closeErr != nil {
				return result, closeErr
			}
			if count > 1 {
				return result, fmt.Errorf("PostgreSQL default for %s.%s references multiple sequences; specify its sequence authority", table, column)
			}
			sequenceOID.Valid = count == 1
		}
	}
	if err != nil {
		return result, fmt.Errorf("resolve PostgreSQL sequence for %s.%s: %w", table, column, err)
	}
	if !sequenceOID.Valid {
		return result, fmt.Errorf("PostgreSQL column %s.%s has no identity, serial/default sequence or explicit sequence authority", table, column)
	}
	if explicit == "" && identityMode == "" {
		var direct bool
		err = q.QueryRowContext(ctx, `SELECT pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) IN (
            pg_catalog.format('nextval(%L::regclass)',$3::oid::regclass::text),
            pg_catalog.format('pg_catalog.nextval(%L::regclass)',$3::oid::regclass::text))
            FROM pg_catalog.pg_attrdef ad JOIN pg_catalog.pg_attribute a ON a.attrelid=ad.adrelid AND a.attnum=ad.adnum
            WHERE ad.adrelid=$1::oid AND a.attname=$2`, tableOID, actualColumn, sequenceOID.Int64).Scan(&direct)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return result, err
		}
		if !direct {
			return result, fmt.Errorf("PostgreSQL default for %s.%s is not a direct nextval; specify an explicit sequence authority", table, column)
		}
	}
	var cycling bool
	err = q.QueryRowContext(ctx, `SELECT current_database(),n.nspname,c.relname,s.seqstart,s.seqincrement,s.seqmax,
 pg_catalog.format_type(s.seqtypid,NULL),s.seqcycle
 FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class c ON c.oid=s.seqrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE s.seqrelid=$1::oid`, sequenceOID.Int64).Scan(
		&result.sequence.Catalog, &result.sequence.Schema, &result.sequence.Name, &result.sequence.StartValue, &result.sequence.IncrementBy, &result.sequence.MaxValue, &result.sequence.DataType, &cycling)
	if err != nil {
		return result, fmt.Errorf("resolve PostgreSQL sequence object for %s.%s (PostgreSQL 10+ required): %w", table, column, err)
	}
	if cycling {
		return result, fmt.Errorf("PostgreSQL sequence %s.%s cycles and cannot provide unique identity reservations", result.sequence.Schema, result.sequence.Name)
	}
	result.oid = sequenceOID.Int64
	return result, nil
}
