package info

import (
	"strings"
	"time"
)

const InsertFlag uint8 = 0x1
const UpdateFlag uint8 = 0x2
const DeleteFlag uint8 = 0x4
const UpsertFlag uint8 = 0x8

const InsertBatchFlag uint8 = 0x1
const InsertWithTransientFlag uint8 = 0x2
const InsertByLoadFlag uint8 = 0x4

const UpdateWithTransientFlag uint8 = 0x1

const DeleteBatchFlag uint8 = 0x1
const DeleteWithTransientFlag uint8 = 0x2

// MergeConfig represents merger config
type MergeConfig interface {
	DummyMergerConfigFn()
}

type MergeResult interface {
	// RowsAffected returns the number of rows affected by an insert, upsert, update or delete.
	RowsAffected() int

	// InsRowsAffected returns the number of rows affected by an insert.
	InsRowsAffected() int

	// UpsRowsAffected returns the number of rows affected by an upsert.
	UpsRowsAffected() int

	// UpdRowsAffected returns the number of rows affected by an update.
	UpdRowsAffected() int

	// DelRowsAffected returns the number of rows affected by an delete.
	DelRowsAffected() int

	// DelRowsAffected returns merge report.
	Report() string

	// InsertingTime returns inserting duration.
	InsertingTime() time.Duration

	// UpsertingTime returns upserting duration.
	UpsertingTime() time.Duration

	// UpdatingTime returns updating duration.
	UpdatingTime() time.Duration

	// DeletingTime returns deleting duration.
	DeletingTime() time.Duration

	// MergingTime returns total merging duration.
	MergingTime() time.Duration

	// MergedTable returns merged table name.
	MergedTable() string
}

// MergeStrategyDesc describes merge strategy
func MergeStrategyDesc(strategy uint8) string {
	sb := strings.Builder{}
	if strategy&InsertFlag == InsertFlag {
		sb.WriteString("ins")
	}

	if strategy&UpdateFlag == UpdateFlag {
		if sb.Len() > 0 {
			sb.WriteString("/")
		}
		sb.WriteString("upd")
	}

	if strategy&UpsertFlag == UpsertFlag {
		if sb.Len() > 0 {
			sb.WriteString("/")
		}
		sb.WriteString("ups")
	}

	if strategy&DeleteFlag == DeleteFlag {
		if sb.Len() > 0 {
			sb.WriteString("/")
		}
		sb.WriteString("del")
	}

	return sb.String()
}
