package info

import "time"

// MergeStrategy represents strategy of merging data
type MergeStrategy string

// MergeInsStrategy represents strategy of inserting when merging data
type MergeInsStrategy string

// MergeUpdStrategy represents strategy of updating when merging data
type MergeUpdStrategy string

// MergeDelStrategy represents strategy of deleting when merging data
type MergeDelStrategy string

// MergeOperationType represents merge operation i.e. INSERT, UPDATE, DELETE
type MergeSubOperationType string

const (
	// MergeStrategyInsUpdDel represents merge strategy that performs insert, update and delete operations
	MergeStrategyInsUpdDel = MergeStrategy("ins_upd_del")
	// MergeStrategyInsDel represents merge strategy that performs insert and delete operations
	MergeStrategyInsDel = MergeStrategy("ins_del")
	// MergeStrategyUpsDel represents merge strategy that performs upsert and delete operations
	MergeStrategyUpsDel = MergeStrategy("ups_del")

	// MergeInsStrategyWithTransient represents insert strategy that uses transient/temporary table
	MergeInsStrategyWithTransient = MergeInsStrategy("insert_with_transient_table")
	// MergeUpdStrategyWithTransient represents update strategy that uses transient/temporary table
	MergeUpdStrategyWithTransient = MergeUpdStrategy("update_with_transient_table")
	// MergeDelStrategyWithTransient represents delete strategy that uses transient/temporary table
	MergeDelStrategyWithTransient = MergeDelStrategy("delete_with_transient_table")

	// MergeDelStrategyDelBatch represents batch delete strategy
	MergeDelStrategyDelBatch = MergeDelStrategy("delete_batch")

	// MergeInsStrategyInsByLoad represents insert strategy that uses loader to insert data
	MergeInsStrategyInsByLoad = MergeInsStrategy("insert_by_load")
	// MergeInsStrategyInsBatch represents batch delete strategy
	MergeInsStrategyInsBatch = MergeInsStrategy("insert_batch")

	// MergeSubOperationTypeInsert represents insert type sub-operation
	MergeSubOperationTypeInsert = MergeSubOperationType("insert")
	// MergeSubOperationTypeUpdate represents update type sub-operation
	MergeSubOperationTypeUpdate = MergeSubOperationType("update")
	// MergeSubOperationTypeDelete represents delete type sub-operation
	MergeSubOperationTypeDelete = MergeSubOperationType("delete")
)

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
}
