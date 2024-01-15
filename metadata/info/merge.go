package info

// PresetMergeStrategy represents strategy of merging data
type PresetMergeStrategy string

// PresetMergeInsStrategy represents strategy of inserting when merging data
type PresetMergeInsStrategy string

// PresetMergeUpdStrategy represents strategy of updating when merging data
type PresetMergeUpdStrategy string

// PresetMergeDelStrategy represents strategy of deleting when merging data
type PresetMergeDelStrategy string

const (
	// PresetMergeStrategyBaseInsUpdDel represents merge strategy that performs insert, update and delete operations
	PresetMergeStrategyBaseInsUpdDel = PresetMergeStrategy("ins_upd_del")
	// PresetMergeStrategyBaseInsDel represents merge strategy that performs insert and delete operations
	PresetMergeStrategyBaseInsDel = PresetMergeStrategy("ins_del")
	// PresetMergeStrategyBaseUpsDel represents merge strategy that performs upsert and delete operations
	PresetMergeStrategyBaseUpsDel = PresetMergeStrategy("ups_del")

	// PresetMergeInsStrategyBase represents insert strategy that uses transient/temporary table
	PresetMergeInsStrategyBase = PresetMergeInsStrategy("insert_with_transient_table")
	// PresetMergeUpdStrategy represents update strategy that uses transient/temporary table
	PresetMergeUpdStrategyBase = PresetMergeUpdStrategy("update_with_transient_table")
	// PresetMergeDelStrategy represents delete strategy that uses transient/temporary table
	PresetMergeDelStrategyBase = PresetMergeDelStrategy("delete_with_transient_table")

	// PresetMergeInsStrategyInsByLoad represents insert strategy that uses loader to insert data
	PresetMergeInsStrategyInsByLoad = PresetMergeInsStrategy("insert_by_load")
)

// MergerConfigs represents merger config interface
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
}
