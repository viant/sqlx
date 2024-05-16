package merge_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/io/merge"
	"github.com/viant/sqlx/loption"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	_ "github.com/viant/sqlx/metadata/product/mysql/load"
	_ "github.com/viant/sqlx/metadata/product/mysql/merge"
	mconfig "github.com/viant/sqlx/metadata/product/mysql/merge/config"
	"github.com/viant/sqlx/moption"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type Rule struct {
	ID           int    `json:"ID" column:"ID" pk:"true" sqlx:"name=ID,primaryKey=true"`
	Tag          string `json:"Tag" column:"TAG" sqlx:"name=TAG"`
	Entity       string `json:"Entity" column:"ENTITY" sqlx:"name=ENTITY,nullifyempty"`
	EntityID     int    `json:"EntityID" column:"ENTITY_ID" sqlx:"name=ENTITY_ID"`
	FeatureType  string `json:"FeatureType" column:"FEATURE_TYPE" sqlx:"name=FEATURE_TYPE"`
	FeatureGroup int    `json:"FeatureGroup" column:"FEATURE_GROUP" sqlx:"name=FEATURE_GROUP"`
	Operator     string `json:"Operator" column:"OPERATOR" sqlx:"name=OPERATOR"`
	FeatureValue string `json:"FeatureValue" column:"FEATURE_VALUE" sqlx:"name=FEATURE_VALUE"`
	//ID           int    `json:"ID" column:"ID" pk:"true" sqlx:"name=ID,primaryKey=true"`
}

// TODO pass tx
func TestService_Exec(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
	}

	type Config struct {
		Driver string
		DSN    string
	}

	c := &Config{
		Driver: "mysql",
		DSN:    dsn,
	}

	var testCases = []struct {
		description       string
		table             string
		options           []moption.Option
		testDataPath      string
		dstRecords        interface{}
		srcRecords        interface{}
		expected          interface{}
		config            info.MergeConfig
		hasError          bool
		ignoreIdAssertion bool
	}{
		{
			description: "merger ins_del strategy, ins/upd/del:use_insert_batch/none/use_transient, all loaders with upsert - matchKey without id, PresetIDWithTransientTransaction",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 0, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 0, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 0, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
				{ID: 0, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 6, FeatureType: "adsize", FeatureGroup: 6, Operator: "=", FeatureValue: "6"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 4, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 5, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
				{ID: 6, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 6, FeatureType: "adsize", FeatureGroup: 6, Operator: "=", FeatureValue: "6"},
			},
			ignoreIdAssertion: true,
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(2), dialect.PresetIDWithTransientTransaction},
				[]uint8{info.DeleteFlag, info.InsertFlag}),
		},
		{
			description: "base ins strategy, empty transient ins/del loader options",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, false, false,
				false, true, false,
				[]loption.Option{},
				[]loption.Option{},
				[]loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
		},
		{
			description: "base ins strategy, empty transient ins/del loader options - repeated src records - error - rollback",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2b", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2b"},
				{ID: 3, Tag: "CI_CAMPAIGN:3a", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3a"},
				{ID: 3, Tag: "CI_CAMPAIGN:3b", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3b"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, false, false,
				false, true, false,
				[]loption.Option{},
				[]loption.Option{},
				[]loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
			hasError: true,
		},
		{
			description: "base ins strategy, empty transient ins/del loader no options - too big id - no error - no rollback - corrupted data",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 9999999999, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 2147483647, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, false, false,
				false, true, false,
				[]loption.Option{},
				[]loption.Option{},
				[]loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
			hasError: false,
		},
		{
			description: "base ins strategy, transient ins/del loaders with upsert option",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, false, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
		},
		//TODO srcRecords problem with maintaining order later (after to map conversion), result can be different every time
		//{
		//	description: "ins by load strategy, transient ins/del loaders with upsert option - repeated src records - no error - replaced data",
		//	table:       "CI_TARGETING_RULE_TEST",
		//	options:     []moption.Option{},
		//	dstRecords: []*Rule{
		//		{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
		//		{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
		//	},
		//	srcRecords: []*Rule{
		//		{ID: 2, Tag: "CI_CREATIVE:2b", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2b"},
		//		{ID: 3, Tag: "CI_CAMPAIGN:3a", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3a"},
		//		{ID: 3, Tag: "CI_CAMPAIGN:3b", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3b"},
		//		{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
		//	},
		//	expected: []*Rule{
		//		{ID: 2, Tag: "CI_CREATIVE:2b", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2b"},
		//		{ID: 3, Tag: "CI_CAMPAIGN:3b", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3b"},
		//		{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
		//	},
		//	ignoreIdAssertion: true,
		//	// OR depened on random sorting later
		//	//expected: []*Rule{
		//	//	{ID: 2, Tag: "CI_CREATIVE:2b", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2b"},
		//	//	{ID: 3, Tag: "CI_CAMPAIGN:3a", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3a"},
		//	//	{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
		//	//},
		//
		//	config: getInfoConfig(
		//		info.InsertFlag | info.DeleteFlag,
		//		false, true, false,
		//		false, true, false,
		//		[]loption.Option{loption.WithUpsert()},
		//		nil,
		//		[]loption.Option{loption.WithUpsert()},
		//		info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
		//		[]loption.Option{loption.WithUpsert()},
		//		[]option.Option{},
		//		nil),
		//	hasError: false,
		//},
		{
			description: "base ins strategy, empty transient ins/del loader options - too big id - error - rollback",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 9999999999, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, false, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
			hasError: true,
		},
		{
			description: "ins by load strategy, transient del loader with upsert option, ins loader with upsert option",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				nil,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
		},
		{
			description: "ins by load strategy, transient del loader with upsert option, ins loader with upsert option - too big id - error - rollback",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 9999999999, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			hasError: true,
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				nil,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "ins by load strategy, transient del loader with upsert option, ins loader with no option - too big id - no error - no rollback - corrupted data",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 9999999999, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 2147483647, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			hasError: true,
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				nil,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{},
				nil),
		},
		//#
		{
			description: "ins batch strategy, transient del loader with upsert option, ins loader with no option",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				nil,
				[]loption.Option{loption.WithUpsert()},
				[]loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{
					option.BatchSize(3), dialect.PresetIDWithTransientTransaction,
				},
				nil),
		},
		{
			description: "ins batch strategy, transient del loader with upsert option, ins loader with no option - too big id - error - rollback",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 9999999999, Tag: "CI_CAMPAIGN:3", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			hasError: true,
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				nil,
				nil,
				[]loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{
					option.BatchSize(3), dialect.PresetIDWithTransientTransaction,
				},
				nil),
		},
		{
			description: "ins batch strategy, empty transient ins/del loader options - repeated src records - error - rollback",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2b", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2b"},
				{ID: 3, Tag: "CI_CAMPAIGN:3a", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3a"},
				{ID: 3, Tag: "CI_CAMPAIGN:3b", Entity: "CI_CAMPAIGN", EntityID: 3, FeatureType: "adsize", FeatureGroup: 3, Operator: "=", FeatureValue: "3b"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, false, false,
				false, true, false,
				[]loption.Option{},
				[]loption.Option{},
				[]loption.Option{},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{dialect.PresetIDWithTransientTransaction},
				nil), // using default batchSize: option.BatchSize(1)
			hasError: true,
		},
		////// match key without id
		{
			description: "merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{}, nil, []loption.Option{},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_del strategy, ins/upd/del:use_upsert_loader/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_del strategy, ins/upd/del:use_insert_batch/none/use_transient, all loaders with upsert - matchKey without id, PresetIDWithTransientTransaction and non-zero id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(2), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_upsert_loader/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:insert_batch/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{option.BatchSize(3), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "merger ups_del strategy, ups/del:use_upsert_loader/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.UpsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				nil, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		///////////////	with id below
		{
			description: "merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with upsert - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		{
			description: "merger ins_del strategy, ins/upd/del:use_upsert_loader/none/use_transient, all loaders with upsert - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with upsert - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		{
			description: "merger ins_upd_del strategy, ins/upd/del:use_upsert_loader/use_transient/use_transient, all loaders with upsert - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		{
			description: "merger ups_del strategy, ups/del:use_upsert_loader/use_transient, all loaders with upsert - matchKey with id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfigWithMatchKeyFn(
				info.UpsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				nil, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				MatchKeyWithIdFn,
				nil),
		},
		// empty src
		//////
		{
			description: "empty src: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{}, nil, []loption.Option{},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_del strategy, ins/upd/del:use_upsert_loader/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_del strategy, ins/upd/del:use_insert_batch/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(3), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty src: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_upd_del strategy, ins/upd/del:use_upsert_loader/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty src: merger ins_upd_del strategy, ins/upd/del:use_insert_batch/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{option.BatchSize(2), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty src: merger ups_del strategy, ups/del:use_upsert_loader/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords: []*Rule{
				{ID: 2, Tag: "CI_CAMPAIGN:1", Entity: "CI_CAMPAIGN", EntityID: 1, FeatureType: "adsize", FeatureGroup: 1, Operator: "=", FeatureValue: "1"},
				{ID: 1, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:303", Entity: "CI_CAMPAIGN", EntityID: 303, FeatureType: "adsize", FeatureGroup: 303, Operator: "=", FeatureValue: "303"},
			},
			srcRecords: []*Rule{},
			expected:   []*Rule{},
			config: getInfoConfig(
				info.UpsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				nil, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		/// empty dst
		{
			description: "empty dst: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{}, nil, []loption.Option{},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_del strategy, ins/upd/del:use_upsert_loader/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_del strategy, ins/upd/del:use_insert_batch/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(3), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty dst: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_upd_del strategy, ins/upd/del:use_upsert_loader/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty dst: merger ins_upd_del strategy, ins/upd/del:use_insert_batch/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(3), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty dst: merger ups_del strategy, ups/del:use_upsert_loader/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			expected: []*Rule{
				{ID: 2, Tag: "CI_CREATIVE:2", Entity: "CI_CREATIVE", EntityID: 2, FeatureType: "adsize", FeatureGroup: 2, Operator: "=", FeatureValue: "2"},
				{ID: 3, Tag: "CI_CAMPAIGN:304", Entity: "CI_CAMPAIGN", EntityID: 304, FeatureType: "adsize", FeatureGroup: 304, Operator: "=", FeatureValue: "304"},
				{ID: 4, Tag: "CI_CAMPAIGN:4", Entity: "CI_CAMPAIGN", EntityID: 4, FeatureType: "adsize", FeatureGroup: 4, Operator: "=", FeatureValue: "4"},
			},
			config: getInfoConfig(
				info.UpsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				nil, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		/// empty src and dst
		/// empty dst
		{
			description: "empty src and dst: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{}, nil, []loption.Option{},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_del strategy, ins/upd/del:use_transient/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				false, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, 0, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_del strategy, ins/upd/del:use_upsert_loader/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_del strategy, ins/upd/del:use_insert_batch/none/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.DeleteFlag,
				true, true, false,
				false, true, false,
				[]loption.Option{loption.WithUpsert()}, nil, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, 0, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(2), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty src and dst: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with no upsert (dangerous) - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{}, []loption.Option{}, []loption.Option{},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_upd_del strategy, ins/upd/del:use_transient/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				false, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertWithTransientFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				nil,
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_upd_del strategy, ins/upd/del:use_upsert_loader/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
		{
			description: "empty src and dst: merger ins_upd_del strategy, ins/upd/del:use_insert_batch/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertBatchFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{},
				[]option.Option{option.BatchSize(2), dialect.PresetIDWithTransientTransaction},
				nil),
		},
		{
			description: "empty src and dst: merger ins_upd_del strategy, ins/upd/del:use_insert_batch/use_transient/use_transient, all loaders with upsert - matchKey without id",
			table:       "CI_TARGETING_RULE_TEST",
			options:     []moption.Option{},
			dstRecords:  []*Rule{},
			srcRecords:  []*Rule{},
			expected:    []*Rule{},
			config: getInfoConfig(
				info.InsertFlag|info.UpdateFlag|info.DeleteFlag,
				true, false, false,
				false, false, false,
				[]loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()}, []loption.Option{loption.WithUpsert()},
				info.InsertByLoadFlag, info.UpdateWithTransientFlag, info.DeleteWithTransientFlag,
				[]loption.Option{loption.WithUpsert()},
				[]option.Option{},
				nil),
		},
	}

	//for i, testCase := range testCases[(len(testCases) - 1):] {
	for i, testCase := range testCases {
		i = i
		//for _, testCase := range testCases[0:1] {

		//fmt.Printf("\n\n")
		//fmt.Println("================================================")
		fmt.Printf("==== TEST CASE: %d %s\n", i, testCase.description)
		//fmt.Println("================================================")

		initSQL := []string{
			"DROP TABLE IF EXISTS `" + testCase.table + "`",
			"CREATE TABLE IF NOT EXISTS `" + testCase.table + "` (\n  `ID` int(11) NOT NULL AUTO_INCREMENT,\n  `TAG` varchar(255) DEFAULT NULL,\n  `ENTITY` varchar(255) DEFAULT NULL,\n  `ENTITY_ID` bigint(20) DEFAULT NULL,\n  `FEATURE_TYPE` varchar(255) DEFAULT NULL,\n  `FEATURE_GROUP` int(11) DEFAULT NULL,\n  `OPERATOR` varchar(255) DEFAULT NULL,\n  `FEATURE_VALUE` varchar(300) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
		}

		db, err := sql.Open(c.Driver, c.DSN)
		assert.Nil(t, err, testCase.description)

		for _, SQL := range initSQL {
			_, err := db.Exec(SQL)
			assert.Nil(t, err, testCase.description)
		}

		rules, ok := testCase.dstRecords.([]*Rule)
		assert.Equal(t, true, ok, testCase.description)

		if len(rules) > 0 {
			loadOptions := []loption.Option{loption.WithUpsert()}
			loader, err := load.New(context.TODO(), db, testCase.table)
			assert.Nil(t, err)

			cnt, err := loader.Exec(context.TODO(), testCase.dstRecords, loadOptions...)
			assert.Nil(t, err)
			assert.NotEqual(t, cnt, 0, testCase.description)
		}

		merger, err := merge.New(context.Background(), db, testCase.table)
		assert.Nil(t, err, testCase.description)

		result, err := merger.Exec(context.TODO(), testCase.srcRecords, testCase.config, testCase.options...)
		if !testCase.hasError {
			assert.Nil(t, err, testCase.description)
		}
		result = result
		//result.Report()

		SQL := "SELECT * FROM " + testCase.table + " ORDER BY ID"

		rows, err := db.QueryContext(context.TODO(), SQL)
		assert.Nil(t, err, testCase.description)
		actual := []*Rule{}

		for rows.Next() {
			rule := Rule{}
			err = rows.Scan(&rule.ID, &rule.Tag, &rule.Entity, &rule.EntityID, &rule.FeatureType, &rule.FeatureGroup, &rule.Operator, &rule.FeatureValue)
			assert.Nil(t, err, testCase.description)
			actual = append(actual, &rule)
		}

		expected := testCase.expected.([]*Rule)

		if testCase.ignoreIdAssertion {
			for _, v := range expected {
				v.ID = 0
			}
			for _, v := range actual {
				v.ID = 0
			}

			sort.Slice(expected, func(i, j int) bool {
				return expected[i].EntityID < expected[j].EntityID
			})

			sort.Slice(actual, func(i, j int) bool {
				return actual[i].EntityID < actual[j].EntityID
			})
		}

		if !assertly.AssertValues(t, expected, actual, testCase.description) {
			fmt.Println("EXPECTED")
			toolbox.DumpIndent(expected, true)
			fmt.Println("ACTUAL")
			toolbox.DumpIndent(actual, true)
		}

		if assertly.AssertValues(t, actual, expected, testCase.description) {
			//fmt.Println("EXPECTED")
			//toolbox.DumpIndent(expected, true)
			//fmt.Println("ACTUAL")
			//toolbox.DumpIndent(actual, true)
		}
	}
}

type cache struct {
	name  string
	time  time.Time
	value []*Rule
}

var testCache *cache

func getTestData(name string, path string) []*Rule {
	if testCache != nil {
		if time.Now().Sub(testCache.time) < time.Second*600 && name == testCache.name {
			fmt.Println("using testCache:", name)
			return testCache.value
		}
	}

	file, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalln(err)
	}
	data := []*Rule{}
	err = json.Unmarshal([]byte(file), &data)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("Tag: %v,\n"+
		"Entity: %v,\n"+
		"EntityID: %v,\n"+
		"FeatureType: %v,\n"+
		"FeatureGroup: %v,\n"+
		"Operator: %v,\n"+
		"FeatureValue: %v,\n"+
		"ID: %v,\n",
		data[0].Tag,
		data[0].Entity,
		data[0].EntityID,
		data[0].FeatureType,
		data[0].FeatureGroup,
		data[0].Operator,
		data[0].FeatureValue,
		data[0].ID)

	testCache = &cache{
		name:  name,
		time:  time.Now(),
		value: data,
	}

	return data
}

func getInfoConfig(mergeStrategy uint8, omitInsTransient bool, omitUpdTransient bool, omitDelTransient bool, omitIns bool, omitUpd bool, omitDel bool, insTransientLoadOptions []loption.Option, updTransientLoadOptions []loption.Option, delTransientLoadOptions []loption.Option, insStartegy uint8, updStartegy uint8, delStartegy uint8, insLoadOptions []loption.Option, insOptions []option.Option, operationOrder []uint8) info.MergeConfig {
	config := &mconfig.Config{
		Strategy:   mergeStrategy,
		MatchKeyFn: MatchKeyFn,
		NewRowFn:   NewRowFn,
		FetchSQL:   "SELECT ID, TAG, ENTITY, ENTITY_ID, FEATURE_TYPE, FEATURE_GROUP, OPERATOR, FEATURE_VALUE FROM CI_TARGETING_RULE_TEST",
		Update: &mconfig.Update{
			Transient: &mconfig.Transient{
				TableName: "CI_TARGETING_RULE_UPD_TMP",
				InitSQL: []string{
					"CREATE TABLE IF NOT EXISTS `CI_TARGETING_RULE_UPD_TMP` (\n  `ID` int(11) NOT NULL,\n  `TAG` varchar(255) DEFAULT NULL,\n  `ENTITY` varchar(255) DEFAULT NULL,\n  `ENTITY_ID` bigint(20) DEFAULT NULL,\n  `FEATURE_TYPE` varchar(255) DEFAULT NULL,\n  `FEATURE_GROUP` int(11) DEFAULT NULL,\n  `OPERATOR` varchar(255) DEFAULT NULL,\n  `FEATURE_VALUE` varchar(300) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
					"TRUNCATE TABLE CI_TARGETING_RULE_UPD_TMP",
				},
				LoadOptions: updTransientLoadOptions,
			},
			/*dst.id BETWEEN %d AND %d AND*/
			UpdateSQL: `UPDATE CI_TARGETING_RULE_TEST dst 
JOIN CI_TARGETING_RULE_UPD_TMP tmp 
ON dst.ID = tmp.ID
SET 
dst.TAG = tmp.TAG, 
dst.ENTITY = tmp.ENTITY, 
dst.ENTITY_ID = tmp.ENTITY_ID, 
dst.FEATURE_TYPE = tmp.FEATURE_TYPE, 
dst.FEATURE_GROUP = tmp.FEATURE_GROUP, 
dst.OPERATOR = tmp.OPERATOR, 
dst.FEATURE_VALUE = tmp.FEATURE_VALUE`,
			UpdateStrategy: updStartegy,
		},
		Insert: &mconfig.Insert{
			Transient: &mconfig.Transient{
				TableName: "CI_TARGETING_RULE_INS_TMP",
				InitSQL: []string{
					"CREATE TABLE IF NOT EXISTS `CI_TARGETING_RULE_INS_TMP` (\n  `ID` int(11) NOT NULL,\n  `TAG` varchar(255) DEFAULT NULL,\n  `ENTITY` varchar(255) DEFAULT NULL,\n  `ENTITY_ID` bigint(20) DEFAULT NULL,\n  `FEATURE_TYPE` varchar(255) DEFAULT NULL,\n  `FEATURE_GROUP` int(11) DEFAULT NULL,\n  `OPERATOR` varchar(255) DEFAULT NULL,\n  `FEATURE_VALUE` varchar(300) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
					"TRUNCATE TABLE CI_TARGETING_RULE_INS_TMP",
				},
				LoadOptions: insTransientLoadOptions,
			},
			InsertSQL: `INSERT INTO CI_TARGETING_RULE_TEST (ID, TAG, ENTITY, ENTITY_ID, FEATURE_TYPE, FEATURE_GROUP, OPERATOR, FEATURE_VALUE) SELECT ID, TAG, ENTITY, ENTITY_ID, FEATURE_TYPE, FEATURE_GROUP, OPERATOR, FEATURE_VALUE FROM CI_TARGETING_RULE_INS_TMP`,
			//InsertStrategy: info.InsertByLoadFlag,
			//InsertStrategy: info.MergeStrategyBase,
			InsertStrategy: insStartegy,
			LoadOptions:    insLoadOptions,
			Options:        insOptions,
		},
		Delete: &mconfig.Delete{
			Transient: &mconfig.Transient{
				TableName: "CI_TARGETING_RULE_DEL_TMP",
				InitSQL: []string{ // CREATE TABLE IF NOT EXISTS `CI_TARGETING_RULE_DEL_TMP` AS SELECT * FROM ...
					"CREATE TABLE IF NOT EXISTS `CI_TARGETING_RULE_DEL_TMP` (\n  `ID` int(11) NOT NULL,\n  `TAG` varchar(255) DEFAULT NULL,\n  `ENTITY` varchar(255) DEFAULT NULL,\n  `ENTITY_ID` bigint(20) DEFAULT NULL,\n  `FEATURE_TYPE` varchar(255) DEFAULT NULL,\n  `FEATURE_GROUP` int(11) DEFAULT NULL,\n  `OPERATOR` varchar(255) DEFAULT NULL,\n  `FEATURE_VALUE` varchar(300) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1",
					"TRUNCATE TABLE CI_TARGETING_RULE_DEL_TMP",
				},

				LoadOptions: delTransientLoadOptions, // TODO try to pass nil

			},
			DeleteStrategy: delStartegy,
			DeleteSQL:      "DELETE t.* FROM CI_TARGETING_RULE_TEST t join CI_TARGETING_RULE_DEL_TMP d on t.id = d.id",
			Options:        []option.Option{option.BatchSize(8000)},
		},
		OperationOrder: operationOrder,
	}

	if omitInsTransient {
		config.Insert.Transient = nil
	}

	if omitUpdTransient {
		config.Update.Transient = nil
	}

	if omitDelTransient {
		config.Delete.Transient = nil
	}

	if omitIns {
		config.Insert = nil
	}

	if omitUpd {
		config.Update = nil
	}

	if omitDel {
		config.Delete = nil
	}

	//printConfig := &PrintConfig{
	//	Strategy:       config.Strategy,
	//	MatchKeyFn:     "",
	//	NewRowFn:       "",
	//	FetchSQL:       config.FetchSQL,
	//	Update:         config.Update,
	//	Insert:         config.Insert,
	//	Delete:         config.Delete,
	//	OperationOrder: config.OperationOrder,
	//}

	//fmt.Println("AAAAAA")
	//toolbox.DumpIndent(printConfig, false)
	//fmt.Println("AAAAAA")

	//var prettyJSON bytes.Buffer
	//b1, err := json.Marshal(printConfig)
	//if err != nil {
	//	fmt.Println("Err1", err.Error())
	//}
	//err = json.Indent(&prettyJSON, b1, "", "\t")
	//if err != nil {
	//	fmt.Println("Err2", err.Error())
	//}
	//fmt.Println(string(prettyJSON.Bytes()))

	return config
}

func MatchKeyFn(entity interface{}) (interface{}, interface{}, error) {
	var builder strings.Builder //TODO use string Buffer?
	rule, ok := entity.(*Rule)
	if !ok {
		return nil, nil, fmt.Errorf("matchkeyfn: expected %T but got %T", rule, entity)
	}
	builder.Reset()
	builder.Grow(80)
	builder.WriteString(rule.Entity)
	builder.WriteString("/")
	builder.WriteString(strconv.Itoa(rule.EntityID))
	builder.WriteString("/")
	builder.WriteString(rule.FeatureType)
	builder.WriteString("/")
	builder.WriteString(strconv.Itoa(rule.FeatureGroup))
	builder.WriteString("/")
	builder.WriteString(rule.Operator)
	builder.WriteString("/")
	builder.WriteString(rule.FeatureValue)
	return builder.String(), rule.ID, nil
}

func MatchKeyWithIdFn(entity interface{}) (interface{}, interface{}, error) {
	var builder strings.Builder //TODO use string Buffer?
	rule, ok := entity.(*Rule)
	if !ok {
		return nil, nil, fmt.Errorf("matchkeyfn: expected %T but got %T", rule, entity)
	}
	builder.Reset()
	builder.Grow(80)
	builder.WriteString(strconv.Itoa(rule.ID))
	builder.WriteString("/")
	builder.WriteString(rule.Entity)
	builder.WriteString("/")
	builder.WriteString(strconv.Itoa(rule.EntityID))
	builder.WriteString("/")
	builder.WriteString(rule.FeatureType)
	builder.WriteString("/")
	builder.WriteString(strconv.Itoa(rule.FeatureGroup))
	builder.WriteString("/")
	builder.WriteString(rule.Operator)
	builder.WriteString("/")
	builder.WriteString(rule.FeatureValue)
	return builder.String(), rule.ID, nil
}

func NewRowFn() interface{} {
	return &Rule{}
}

func multiplyRules(dbRules []*Rule, multiplier int, percent int) []*Rule {
	currentLen := len(dbRules)
	//fmt.Println("MULTIPLIER: ", multiplier)
	now := time.Now().Format("2006-01-02 15:04:05")
	//fmt.Println("DIFF VALUE: ", now)

	// base

	if multiplier == 0 {
		multiplier = 1
	}

	newDbRules := make([]*Rule, currentLen*multiplier)
	idCnt := 1
	if multiplier == 1 {
		copy(newDbRules, dbRules)
	} else {
		for i, _ := range dbRules {
			for k := 0; k < multiplier; k++ {
				newRule := *dbRules[i]
				newRule.EntityID = newRule.EntityID*10000 + k
				newRule.ID = idCnt
				idCnt++
				newDbRules[i*multiplier+k] = &newRule
			}
		}
	}

	if percent == 0 {
		return newDbRules
	}

	percentCnt := (len(newDbRules) * percent) / 100
	if percentCnt == 0 {
		return newDbRules
	}

	chunk := len(newDbRules) / percentCnt
	if chunk == 0 {
		return newDbRules
	}

	modified := 0
	for i := 0; i < len(newDbRules); i = i + chunk {
		r := &Rule{
			Tag:          newDbRules[i].Tag,
			Entity:       newDbRules[i].Entity,
			EntityID:     newDbRules[i].EntityID,
			FeatureType:  newDbRules[i].FeatureType,
			FeatureGroup: newDbRules[i].FeatureGroup,
			Operator:     newDbRules[i].Operator,
			FeatureValue: newDbRules[i].FeatureValue + " " + now,
			ID:           newDbRules[i].ID,
		}
		newDbRules[i] = r
		modified++
	}

	//if modified > 0 {
	//	fmt.Println("-- preparing test data --")
	//	fmt.Printf("all src rules cnt: %d\n modified cnt: %d\n", len(newDbRules), modified)
	//	fmt.Println("-------------------------")
	//}

	return newDbRules
}

var srcRuleCache *cache

func prepareSrcRules(name string, dbRules []*Rule, toInsCnt, toUpdCnt int, toDelCnt int) []*Rule {
	if srcRuleCache != nil {
		if time.Now().Sub(srcRuleCache.time) < time.Second*2 && name == srcRuleCache.name {
			fmt.Println("using srcRuleCache:", name)
			return srcRuleCache.value
		}
	}

	dbRulesCnt := len(dbRules)
	if toUpdCnt+toDelCnt > dbRulesCnt {
		return nil
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	newLen := dbRulesCnt - toDelCnt + toInsCnt
	newDbRules := make([]*Rule, newLen)

	// DEL
	deleted := 0
	newDbRulesCnt := 0

	if toDelCnt*2 <= dbRulesCnt {
		for i, rule := range dbRules {
			if i%2 == 0 && deleted < toDelCnt {
				deleted++
				continue
			}
			newDbRules[newDbRulesCnt] = rule
			newDbRulesCnt++
		}
	} else {
		for _, rule := range dbRules {
			if deleted < toDelCnt {
				deleted++
				continue
			}
			newDbRules[newDbRulesCnt] = rule
			newDbRulesCnt++
		}
	}

	// UPD
	updated := 0
	if toUpdCnt*2 <= newDbRulesCnt {
		for i := 0; i < newDbRulesCnt && updated < toUpdCnt; i = i + 2 {
			r := &Rule{
				Tag:          newDbRules[i].Tag,
				Entity:       newDbRules[i].Entity,
				EntityID:     newDbRules[i].EntityID,
				FeatureType:  newDbRules[i].FeatureType,
				FeatureGroup: newDbRules[i].FeatureGroup,
				Operator:     newDbRules[i].Operator,
				FeatureValue: "#UPD# " + newDbRules[i].FeatureValue + " " + now,
				ID:           newDbRules[i].ID,
			}
			newDbRules[i] = r
			updated++
		}
	} else {
		for i := 0; i < updated; i++ {
			r := &Rule{
				Tag:          newDbRules[i].Tag,
				Entity:       newDbRules[i].Entity,
				EntityID:     newDbRules[i].EntityID,
				FeatureType:  newDbRules[i].FeatureType,
				FeatureGroup: newDbRules[i].FeatureGroup,
				Operator:     newDbRules[i].Operator,
				FeatureValue: "#UPD# " + newDbRules[i].FeatureValue + " " + now,
				ID:           newDbRules[i].ID,
			}
			newDbRules[i] = r
			updated++
		}
	}

	// INS
	m := newDbRulesCnt - 1
	newID := newDbRules[m].ID
	inserted := 0

	for i := 0; i < toInsCnt; i++ {
		newID++
		r := &Rule{
			Tag:          newDbRules[m].Tag,
			Entity:       newDbRules[m].Entity,
			EntityID:     newDbRules[m].EntityID,
			FeatureType:  newDbRules[m].FeatureType,
			FeatureGroup: newDbRules[m].FeatureGroup,
			Operator:     newDbRules[m].Operator,
			FeatureValue: "#INS# " + newDbRules[m].FeatureValue + " " + now,
			ID:           newID,
		}
		if m >= 1 {
			m--
		}
		newDbRules[newDbRulesCnt] = r
		newDbRulesCnt++
		inserted++
	}

	//fmt.Printf("-- PREPARING SRC DATA SUMMARY--\n")
	//fmt.Printf(" Input rules cnt: %d\n", dbRulesCnt)
	//fmt.Printf("Output rules cnt: %d == %d \n", len(newDbRules), newDbRulesCnt)
	//fmt.Printf("        inserted: %d of %d\n", inserted, toInsCnt)
	//fmt.Printf("         updated: %d of %d\n", updated, toUpdCnt)
	//fmt.Printf("         deleted: %d of %d\n", deleted, toDelCnt)
	//fmt.Println("-------------------------")

	srcRuleCache = &cache{
		name:  name,
		time:  time.Now(),
		value: newDbRules,
	}

	return newDbRules
}

func getInfoConfigWithMatchKeyFn(mergeStrategy uint8, omitInsTransient bool, omitUpdTransient bool, omitDelTransient bool, omitIns bool, omitUpd bool, omitDel bool, insTransientLoadOptions []loption.Option, updTransientLoadOptions []loption.Option, delTransientLoadOptions []loption.Option, insStartegy uint8, updStartegy uint8, delStartegy uint8, insLoadOptions []loption.Option, insOptions []option.Option, matchKeyFn func(entity interface{}) (interface{}, interface{}, error), operationOrder []uint8) info.MergeConfig {
	config := getInfoConfig(
		mergeStrategy,
		omitInsTransient,
		omitUpdTransient,
		omitDelTransient,
		omitIns,
		omitUpd,
		omitDel,
		insTransientLoadOptions,
		updTransientLoadOptions,
		delTransientLoadOptions,
		insStartegy,
		updStartegy,
		delStartegy,
		insLoadOptions,
		insOptions,
		operationOrder)
	mconfig, ok := config.(*mconfig.Config)
	if ok {
		mconfig.MatchKeyFn = matchKeyFn
	}
	return mconfig
}

// /
type PrintConfig struct {
	Strategy   uint8
	MatchKeyFn string
	NewRowFn   string
	FetchSQL   string

	Update         *mconfig.Update
	Insert         *mconfig.Insert
	Delete         *mconfig.Delete
	OperationOrder []uint8
}
