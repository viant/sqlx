package metadata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/registry"
)

func TestDetectProduct_ValidVersionIsCached(t *testing.T) {
	db, driver := openVersionDetectionDB(t, versionDetectionResponse{
		rows: [][]driver.Value{{"VersionDetectionTest - 3.45.1"}},
	})
	service := New()
	for range 2 {
		product, err := detectProductWithoutPanic(t, service, db)
		if err != nil {
			t.Fatalf("DetectProduct() error = %v", err)
		}
		assertDetectedVersion(t, product)
	}
	if got := driver.queries.Load(); got != 1 {
		t.Fatalf("version query count = %d, want 1 after cached detection", got)
	}
}

func TestDetectProduct_VersionFailuresAreNotCached(t *testing.T) {
	transportErr := errors.New("version row transport failure")
	testCases := []struct {
		name        string
		response    versionDetectionResponse
		wantError   error
		wantMessage string
	}{
		{name: "malformed", response: versionDetectionResponse{rows: [][]driver.Value{{"VersionDetectionTest - unknown"}}}},
		{name: "empty_version", response: versionDetectionResponse{rows: [][]driver.Value{{""}}}},
		{name: "zero_rows"},
		{name: "first_row_transport_error", response: versionDetectionResponse{rowError: transportErr}, wantError: transportErr},
		{name: "first_row_deadline", response: versionDetectionResponse{rowError: context.DeadlineExceeded}, wantError: context.DeadlineExceeded},
		{name: "first_row_canceled", response: versionDetectionResponse{rowError: context.Canceled}, wantError: context.Canceled},
		{name: "scan_error", response: versionDetectionResponse{rows: [][]driver.Value{{nil}}}, wantMessage: "Scan error"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, driver := openVersionDetectionDB(t, testCase.response, versionDetectionResponse{
				rows: [][]driver.Value{{"VersionDetectionTest - 3.45.1"}},
			})
			service := New()
			product, err := detectProductWithoutPanic(t, service, db)
			if err == nil {
				t.Fatal("DetectProduct() error = nil, want failed version detection")
			}
			if product != nil {
				t.Fatalf("failed DetectProduct() product = %+v, want nil", product)
			}
			if testCase.wantError != nil && !errors.Is(err, testCase.wantError) {
				t.Fatalf("DetectProduct() error = %v, want wrapped %v", err, testCase.wantError)
			}
			if testCase.wantMessage != "" && !strings.Contains(err.Error(), testCase.wantMessage) {
				t.Fatalf("DetectProduct() error = %v, want message containing %q", err, testCase.wantMessage)
			}
			product, err = detectProductWithoutPanic(t, service, db)
			if err != nil {
				t.Fatalf("DetectProduct() retry error = %v", err)
			}
			assertDetectedVersion(t, product)
			if got := driver.queries.Load(); got != 2 {
				t.Fatalf("version query count = %d, want 2 after retry", got)
			}
		})
	}
}

func detectProductWithoutPanic(t *testing.T, service *Service, db *sql.DB) (*database.Product, error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("DetectProduct() panicked: %v", recovered)
		}
	}()
	return service.DetectProduct(context.Background(), db)
}

func assertDetectedVersion(t *testing.T, product *database.Product) {
	t.Helper()
	if product == nil || product.Name != "VersionDetectionTest" || product.Major != 3 || product.Minor != 45 || product.Release != 1 {
		t.Fatalf("detected product = %+v, want VersionDetectionTest 3.45.1", product)
	}
}

// These drivers never open a socket or access a database. Responses are selected
// per query so a failed cold detection can be retried on the same DB and service.
type versionDetectionResponse struct {
	rows     [][]driver.Value
	rowError error
}

type versionDetectionDriver struct {
	responses []versionDetectionResponse
	queries   atomic.Int32
}

type versionDetectionConnector struct{ driver *versionDetectionDriver }
type versionDetectionConn struct{ driver *versionDetectionDriver }
type versionDetectionStmt struct{ driver *versionDetectionDriver }
type versionDetectionRows struct {
	response versionDetectionResponse
	index    int
}

var registerVersionDetectionProduct sync.Once

func openVersionDetectionDB(t *testing.T, responses ...versionDetectionResponse) (*sql.DB, *versionDetectionDriver) {
	t.Helper()
	registerVersionDetectionProduct.Do(func() {
		product := database.Product{Name: "VersionDetectionTest", Major: 3}
		if err := registry.Register(info.NewQuery(info.KindVersion, "SELECT version", product)); err != nil {
			t.Fatal(err)
		}
		registry.RegisterDialect(&info.Dialect{Product: product})
		typ := reflect.TypeFor[versionDetectionDriver]()
		if err := registry.RegisterDriver(product, typ.PkgPath(), typ.Name()); err != nil {
			t.Fatal(err)
		}
	})
	driver := &versionDetectionDriver{responses: responses}
	db := sql.OpenDB(&versionDetectionConnector{driver: driver})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return db, driver
}

func (d *versionDetectionDriver) Open(string) (driver.Conn, error) {
	return &versionDetectionConn{driver: d}, nil
}

func (c *versionDetectionConnector) Driver() driver.Driver { return c.driver }

func (c *versionDetectionConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}

func (c *versionDetectionConn) Prepare(string) (driver.Stmt, error) {
	return &versionDetectionStmt{driver: c.driver}, nil
}

func (c *versionDetectionConn) Close() error { return nil }

func (c *versionDetectionConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}

func (s *versionDetectionStmt) Close() error  { return nil }
func (s *versionDetectionStmt) NumInput() int { return 0 }

func (s *versionDetectionStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("unexpected execution")
}

func (s *versionDetectionStmt) Query([]driver.Value) (driver.Rows, error) {
	index := int(s.driver.queries.Add(1)) - 1
	if index >= len(s.driver.responses) {
		return nil, errors.New("unexpected additional version query")
	}
	return &versionDetectionRows{response: s.driver.responses[index]}, nil
}

func (r *versionDetectionRows) Columns() []string { return []string{"version"} }
func (r *versionDetectionRows) Close() error      { return nil }

func (r *versionDetectionRows) Next(values []driver.Value) error {
	if r.index < len(r.response.rows) {
		copy(values, r.response.rows[r.index])
		r.index++
		return nil
	}
	if r.response.rowError != nil {
		return r.response.rowError
	}
	return io.EOF
}
