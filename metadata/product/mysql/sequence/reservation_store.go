package sequence

import (
	"context"
	"database/sql"
	"fmt"
)

const reservationSchema = "sqlx_allocator"
const reservationTable = "`sqlx_allocator`.`sequence_reservations`"

// Store provisions native allocator metadata outside application transactions.
// MySQL DDL implicitly commits: allocation never invokes Install, creates source
// objects, modifies source schema, or borrows a second caller connection.
type Store struct{}

func (*Store) Install(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `sqlx_allocator`"); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+reservationTable+` (
 table_schema VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 table_name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 column_name VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 value BIGINT NOT NULL,
 PRIMARY KEY(table_schema,table_name,column_name)
 ) ENGINE=InnoDB`)
	return err
}

func (*Store) check(ctx context.Context, q queryer) error {
	var engine string
	err := q.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", reservationSchema, "sequence_reservations").Scan(&engine)
	if err != nil {
		return fmt.Errorf("MySQL native allocator is not provisioned; run sequence.Store.Install outside writer transactions: %w", err)
	}
	if engine != "InnoDB" {
		return fmt.Errorf("MySQL native allocator requires InnoDB, found %s", engine)
	}
	return nil
}
