package insert_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/testutil/reservationdb"
)

// Exercise the physical metadata filters with mixed table/column spelling,
// a single connection and an authored autoincrement tag on a non-native key.
func TestMySQLOriginalLivePhysicalColumnAuthority(t *testing.T) {
	h := reservationdb.OpenTransient(t)
	h.DB.SetMaxOpenConns(1)
	assertNoAllocatorTable(t, h.DB)
	h.Exec(t, "CREATE TABLE "+h.Table("NativeAuthority")+" (ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,name VARCHAR(64) NOT NULL) ENGINE=InnoDB",
		"CREATE TABLE "+h.Table("OrdinaryAuthority")+" (ID BIGINT NOT NULL PRIMARY KEY,name VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lowerCase int
	if err := h.DB.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCase); err != nil {
		t.Fatal(err)
	}
	var table, before string
	if err := h.DB.QueryRowContext(ctx, "SHOW CREATE TABLE "+h.Table("NativeAuthority")).Scan(&table, &before); err != nil {
		t.Fatal(err)
	}
	service, err := insert.New(ctx, h.DB, h.Table("NativeAuthority"))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := service.SequenceInfo(ctx, &liveReservationRow{})
	if err != nil || metadata == nil || metadata.Schema != h.Schema || !strings.Contains(metadata.Name, "`ID`") {
		t.Fatalf("physical column identity lost: %+v %v", metadata, err)
	}
	var after string
	if err := h.DB.QueryRowContext(ctx, "SHOW CREATE TABLE "+h.Table("NativeAuthority")).Scan(&table, &after); err != nil || after != before {
		t.Fatalf("metadata lookup changed the source: %v", err)
	}
	wrongCase, err := insert.New(ctx, h.DB, h.Table("nativeauthority"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = wrongCase.SequenceInfo(ctx, &liveReservationRow{})
	if lowerCase == 0 && err == nil || lowerCase != 0 && err != nil {
		t.Fatalf("table case authority changed with lower_case_table_names=%d: %v", lowerCase, err)
	}
	ordinary, err := insert.New(ctx, h.DB, h.Table("OrdinaryAuthority"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ordinary.SequenceInfo(ctx, &liveReservationRow{}); err == nil || !strings.Contains(err.Error(), "not AUTO_INCREMENT") {
		t.Fatalf("authored tag acquired non-native column authority: %v", err)
	}
	assertNoAllocatorTable(t, h.DB)
}
