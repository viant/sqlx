package insert

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

func identityIndex(columns []io.Column, name string) int {
	for i, column := range columns {
		if column.Name() == name {
			return i
		}
	}
	return -1
}

func TestNumericSequencer_PrepareUsesFirstZeroIdentityRecord(t *testing.T) {
	type entity struct {
		Bar  int  `sqlx:"bar"`
		ID   *int `sqlx:"name=foo_id,generator=autoincrement"`
		Note int  `sqlx:"note"`
	}

	id1, id2, id4 := 7, 8, 20
	zero := 0
	records := []*entity{
		{Bar: 1, ID: &id1, Note: 11},
		{Bar: 2, ID: &id2, Note: 22},
		{Bar: 3, ID: nil, Note: 33},
		{Bar: 4, ID: &id4, Note: 44},
		{Bar: 5, ID: &zero, Note: 55},
	}

	columns, binder, err := io.StructColumnMapper(&entity{})
	require.NoError(t, err)
	idIndex := identityIndex(columns, "foo_id")
	require.NotEqual(t, -1, idIndex)

	sess := &session{
		binder:  binder,
		columns: columns,
	}

	updater := newNumericSequencer(sess, columns[idIndex], idIndex)
	valueAt, count, err := io.Values(records)
	require.NoError(t, err)

	_, err = updater.prepare(context.Background(), nil, sess, valueAt, count)
	require.NoError(t, err)
	require.Equal(t, 2, updater.presetRecordCount)
	require.Same(t, records[2], updater.presetRecord)
}

func TestNumericSequencer_UpdateRecordOnlyAssignsZeroValues(t *testing.T) {
	type entity struct {
		Bar  int  `sqlx:"bar"`
		ID   *int `sqlx:"name=foo_id,generator=autoincrement"`
		Note int  `sqlx:"note"`
	}

	id1 := 7
	records := []*entity{
		{Bar: 1, ID: &id1, Note: 11},
		{Bar: 2, ID: nil, Note: 22},
	}

	columns, binder, err := io.StructColumnMapper(&entity{})
	require.NoError(t, err)
	idIndex := identityIndex(columns, "foo_id")
	require.NotEqual(t, -1, idIndex)

	sess := &session{
		binder:  binder,
		columns: columns,
	}

	updater := newNumericSequencer(sess, columns[idIndex], idIndex)
	updater.detectedPreset = true
	updater.shallPresetIdentities = true
	updater.sequence = &sink.Sequence{IncrementBy: 1}
	seqValue := int64(101)
	updater.sequenceValue = &seqValue

	firstValues := make([]interface{}, len(columns))
	sess.binder(records[0], firstValues, 0, len(columns))
	err = updater.updateRecord(context.Background(), sess, records[0], &firstValues[idIndex], len(records), nil, nil)
	require.NoError(t, err)
	require.Equal(t, 7, *records[0].ID)
	require.Equal(t, int64(101), *updater.sequenceValue)

	secondValues := make([]interface{}, len(columns))
	sess.binder(records[1], secondValues, 0, len(columns))
	err = updater.updateRecord(context.Background(), sess, records[1], &secondValues[idIndex], len(records), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, records[1].ID)
	require.Equal(t, 101, *records[1].ID)
	require.Equal(t, int64(102), *updater.sequenceValue)
}

func TestNumericSequencer_TransientBuilderNullsIdentityPosition(t *testing.T) {
	type entity struct {
		Bar  int  `sqlx:"bar"`
		ID   *int `sqlx:"name=foo_id,generator=autoincrement"`
		Note int  `sqlx:"note"`
	}

	columns, binder, err := io.StructColumnMapper(&entity{})
	require.NoError(t, err)
	idIndex := identityIndex(columns, "foo_id")
	require.NotEqual(t, -1, idIndex)

	dialect := &info.Dialect{
		Placeholder:               "?",
		SpecialKeywordEscapeQuote: '"',
	}
	builder, err := NewBuilder("foo", io.Columns(columns).Names(), dialect, "foo_id", 1)
	require.NoError(t, err)

	sess := &session{
		Config: &config.Config{
			TableName: "foo",
			Identity:  "foo_id",
			Dialect:   dialect,
			Builder:   builder,
		},
		binder:  binder,
		columns: columns,
	}

	record := &entity{Bar: 3, Note: 33}
	updater := newNumericSequencer(sess, columns[idIndex], idIndex)
	sql, _, err := updater.transientDMLBuilder(sess, record, make([]interface{}, len(columns)), 1)(&sink.Sequence{})
	require.NoError(t, err)
	require.Len(t, sql.Args, len(columns))
	require.Nil(t, sql.Args[idIndex])
}
