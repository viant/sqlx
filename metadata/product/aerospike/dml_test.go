package aerospike

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/aerospike"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"log"
	"os"
	"testing"
)

func TestService_Exec_Mysql(t *testing.T) {
	driver := "aerospike"
	dsn := os.Getenv("TEST_AEROSPIKE_DSN")

	dsn = "aerospike://127.0.0.1:3000/test"

	if dsn == "" {
		t.Skip("set TEST_AEROSPIKE_DSN before running test")
	}

	type parameterizedQuery struct {
		SQL    string
		params []interface{}
	}

	type Message struct {
		Id   int    `aerospike:"id,pk=true" `
		Seq  int    `aerospike:"seq,listKey=true" `
		Body string `aerospike:"body"`
	}

	type SimpleAgg struct {
		Id     int `aerospike:"id,pk=true" `
		Amount int `aerospike:"amount" `
	}

	type Agg struct {
		Id     int `aerospike:"id,pk=true" `
		Seq    int `aerospike:"seq,key=true" `
		Amount int `aerospike:"amount" `
		Val    int `aerospike:"val" `
	}

	var actual []interface{}

	var useCases = []struct {
		description string
		table       string
		options     []option.Option
		records     interface{}
		expect      interface{}
		initSQL     []string
		initParams  []interface{}
		affected    int64
		lastID      int64
		sets        []*parameterizedQuery
		query       string
		queryParams []interface{}
		newItemFn   func() interface{}
		emitFn      func(row interface{}) error
	}{
		{
			description: "01. list insert with index criteria",
			initSQL: []string{
				"DELETE FROM Msg",
			},
			initParams: []interface{}{},
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET Msg AS ?", params: []interface{}{Message{}}},
			},
			table: "Msg$Items",
			records: []*Message{
				{Id: 1, Seq: 0, Body: "test message"},
				{Id: 1, Seq: 2, Body: "last message"},
			},
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDStrategyIgnore,
			},
			// TODO doesn't work from here, but works from aerospike driver test
			//query:       "SELECT id,seq,body FROM Msg$Items WHERE PK = ? AND KEY IN (?,?)",
			//queryParams: []interface{}{1, 0, 2},
			query:       "SELECT id,seq,body FROM Msg$Items WHERE PK = ?",
			queryParams: []interface{}{1},
			newItemFn:   func() interface{} { return &Message{} },
			emitFn: func(row interface{}) error {
				agg := row.(*Message)
				actual = append(actual, agg)
				return nil
			},
			expect: []interface{}{
				&Message{Id: 1, Seq: 0, Body: "test message"},
				&Message{Id: 1, Seq: 2, Body: "last message"},
			},
			affected: 2,
			lastID:   2,
		},
		{
			description: "02. batch merge",
			initSQL: []string{
				"DELETE FROM SimpleAgg",
				"INSERT INTO SimpleAgg(id,amount) VALUES(1,1)",
				"INSERT INTO SimpleAgg(id,amount) VALUES(2,1)",
				"INSERT INTO SimpleAgg(id,amount) VALUES(3,1)",
			},
			initParams: []interface{}{},
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET SimpleAgg AS ?", params: []interface{}{SimpleAgg{}}},
			},
			table: "SimpleAgg",
			records: []*SimpleAgg{
				{Id: 1, Amount: 11},
				{Id: 2, Amount: 12},
				{Id: 3, Amount: 33},
			},
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDStrategyIgnore,
				option.OnDuplicateKeySql("AS new ON DUPLICATE KEY UPDATE amount = amount + new.amount"),
			},
			query:       "SELECT id,amount FROM SimpleAgg WHERE PK IN(?,?,?)",
			queryParams: []interface{}{1, 2, 3},
			newItemFn:   func() interface{} { return &SimpleAgg{} },
			emitFn: func(row interface{}) error {
				agg := row.(*SimpleAgg)
				actual = append(actual, agg)
				return nil
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 12},
				&SimpleAgg{Id: 2, Amount: 13},
				&SimpleAgg{Id: 3, Amount: 34},
			},
			affected: 3,
			lastID:   -1, //TODO
		},
		{
			description: "03. batch merge with map",
			initSQL: []string{
				"DELETE FROM Agg",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,1,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(1,2,1,1)",
				"INSERT INTO Agg$Values(id,seq,amount,val) VALUES(2,1,1,1)",
			},
			initParams: []interface{}{},
			sets: []*parameterizedQuery{
				{SQL: "REGISTER GLOBAL SET Agg AS ?", params: []interface{}{Agg{}}},
			},
			table: "Agg$Values",
			records: []*Agg{
				{Id: 1, Seq: 1, Amount: 11, Val: 111},
				{Id: 1, Seq: 2, Amount: 12, Val: 121},
				{Id: 2, Seq: 1, Amount: 11, Val: 111},
			},
			options: []option.Option{
				option.BatchSize(3),
				dialect.PresetIDStrategyIgnore,
				option.OnDuplicateKeySql("AS new ON DUPLICATE KEY UPDATE val = val + new.val, amount = amount + new.amount"),
			},
			query:       "SELECT id,seq,amount,val FROM Agg$Values WHERE PK = ? AND KEY IN(?, ?)",
			queryParams: []interface{}{1, 1, 2},
			newItemFn:   func() interface{} { return &Agg{} },
			emitFn: func(row interface{}) error {
				agg := row.(*Agg)
				actual = append(actual, agg)
				return nil
			},
			expect: []interface{}{
				&Agg{Id: 1, Seq: 1, Amount: 12, Val: 112},
				&Agg{Id: 1, Seq: 2, Amount: 13, Val: 122},
			},
			affected: 3,
			lastID:   -1, // TODO
		},
		{
			description: "04. batch insert",
			initSQL: []string{
				"DELETE FROM SimpleAgg",
			},
			initParams: []interface{}{},
			sets: []*parameterizedQuery{
				{SQL: "REGISTER SET SimpleAgg AS ?", params: []interface{}{SimpleAgg{}}},
			},
			table: "SimpleAgg",
			records: []*SimpleAgg{
				{Id: 1, Amount: 10},
				{Id: 2, Amount: 20},
			},
			options: []option.Option{
				option.BatchSize(2),
				dialect.PresetIDStrategyIgnore,
				//dialect.PresetIDWithTransientTransaction,
			},
			query:       "SELECT id,amount FROM SimpleAgg WHERE PK IN(?, ?)",
			queryParams: []interface{}{1, 2},
			newItemFn:   func() interface{} { return &SimpleAgg{} },
			emitFn: func(row interface{}) error {
				agg := row.(*SimpleAgg)
				actual = append(actual, agg)
				return nil
			},
			expect: []interface{}{
				&SimpleAgg{Id: 1, Amount: 10},
				&SimpleAgg{Id: 2, Amount: 20},
			},
			affected: 2,
			lastID:   -1, //TODO
		},
	}

outer:
	for _, tc := range useCases {
		ctx := context.TODO()
		var db *sql.DB
		db, err := sql.Open(driver, dsn)

		// register sets
		for _, set := range tc.sets {
			_, err = db.ExecContext(context.Background(), set.SQL, set.params...)
			if !assert.Nil(t, err, tc.description) {
				return
			}
		}

		// TODO
		//tx, err := db.Begin()
		//if !assert.Nil(t, err, tc.description) {
		//	continue
		//}

		// init data
		for _, SQL := range tc.initSQL {
			_, err = db.ExecContext(context.Background(), SQL, tc.initParams...)
			if !assert.Nil(t, err, tc.description) {
				continue outer
			}
		}

		// insert data
		inserter, err := insert.New(context.TODO(), db, tc.table, tc.options...)
		if !assert.Nil(t, err, tc.description) {
			continue
		}

		//tc.options = append(tc.options, tx)
		affected, lastID, err := inserter.Exec(context.TODO(), tc.records, tc.options...)
		assert.Nil(t, err, tc.description)
		assert.EqualValues(t, tc.affected, affected, tc.description) //TODO
		assert.EqualValues(t, tc.lastID, lastID, tc.description)     //TODO

		// read data
		actual = []interface{}{}

		reader, err := read.New(ctx, db, tc.query, tc.newItemFn)
		if err != nil {
			log.Fatalln(err)
		}

		err = reader.QueryAll(ctx, tc.emitFn, tc.queryParams...)
		if !assert.Nil(t, err, tc.description) {
			continue
		}
		assert.Equal(t, tc.expect, actual, tc.description)
	}
}
