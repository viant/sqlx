package sequence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/option"
)

var identityMetadataUnavailable = errors.New("identity metadata fixture unavailable")

type identityMetadataDriver struct{}
type identityMetadataConn struct{ scenario string }
type identityMetadataRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func init() { sql.Register("sqlx_mysql_identity_classification", identityMetadataDriver{}) }
func (identityMetadataDriver) Open(s string) (driver.Conn, error) {
	return &identityMetadataConn{scenario: s}, nil
}
func (*identityMetadataConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepare")
}
func (*identityMetadataConn) Close() error { return nil }
func (*identityMetadataConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("unexpected transaction")
}
func (c *identityMetadataConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.HasPrefix(query, "SELECT DATABASE()") {
		return &identityMetadataRows{columns: []string{"database", "lowercase", "increment", "offset", "mode"}, values: [][]driver.Value{{"fixture", int64(0), int64(1), int64(1), ""}}}, nil
	}
	if !strings.Contains(query, "information_schema.COLUMNS") {
		return nil, fmt.Errorf("unexpected metadata query")
	}
	if c.scenario == "failure" {
		return nil, identityMetadataUnavailable
	}
	rows := &identityMetadataRows{columns: []string{"schema", "table", "column", "kind", "columnType", "extra", "engine"}}
	if c.scenario != "missing" {
		rows.values = [][]driver.Value{{"fixture", "details", "parent_id", "bigint", "bigint", "", "InnoDB"}}
	}
	return rows, nil
}
func (r *identityMetadataRows) Columns() []string { return r.columns }
func (*identityMetadataRows) Close() error        { return nil }
func (r *identityMetadataRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func TestReservationMetadata_NonIdentityClassification(t *testing.T) {
	type useCase struct {
		description, scenario string
		expected              error
		diagnostic            string
	}
	for _, tc := range []useCase{
		{"supplied natural key has no allocator", "natural", metadata.ErrSequenceReservationUnsupported, "fixture.details.parent_id is not AUTO_INCREMENT"},
		{"missing column keeps lookup error", "missing", sql.ErrNoRows, "resolve MySQL sequence column"},
		{"metadata failure is not unsupported allocation", "failure", identityMetadataUnavailable, "resolve MySQL sequence column"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			db, err := sql.Open("sqlx_mysql_identity_classification", tc.scenario)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = (&ReservationMetadata{}).resolve(context.Background(), db, option.Options{option.SequenceTable("details"), option.SequenceColumn("parent_id")})
			if !errors.Is(err, tc.expected) {
				t.Fatalf("error=%v, expected classification %v", err, tc.expected)
			}
			if tc.expected != metadata.ErrSequenceReservationUnsupported && errors.Is(err, metadata.ErrSequenceReservationUnsupported) {
				t.Fatalf("lookup failure was classified as unsupported: %v", err)
			}
			if !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("missing contextual diagnostic: %v", err)
			}
		})
	}
}
