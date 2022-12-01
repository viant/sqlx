package batcher_test

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/insert/batcher"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/toolbox"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type collectTestCase struct {
	description string
	table       string
	initSQL     []string
	recordsCnt  int
	config      *batcher.Config
	concurrency bool
}

type entity struct {
	ID   int    `sqlx:"name=foo_id,primaryKey=true,generator=autoincrement"`
	Name string `sqlx:"foo_name"`
	Bar  float64
}

func TestService_Collect(t *testing.T) {

	driver := "mysql"
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
		return
	}

	aInitSQL := []string{
		"DROP TABLE IF EXISTS t21",
		"CREATE TABLE t21 ( foo_id INTEGER AUTO_INCREMENT PRIMARY KEY, foo_name TEXT, bar INTEGER)",
	}

	var useCases = []*collectTestCase{
		{
			description: "1 - Collect - Concurrency: true, recordsCnt: 50, MaxElements: 1, MaxDurationMs: 1, BatchSize: 2",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  50,
			config: &batcher.Config{
				MaxElements:   1,
				MaxDurationMs: 1,
				BatchSize:     1,
			},
			concurrency: true,
		},
		{
			description: "2 - Collect - Concurrency: false, recordsCnt: 50, MaxElements: 1, MaxDurationMs: 1, BatchSize: 2",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  50,
			config: &batcher.Config{
				MaxElements:   1,
				MaxDurationMs: 1,
				BatchSize:     1,
			},
			concurrency: false,
		},
		{
			description: "3 - Collect - Concurrency: true, recordsCnt: 100, MaxElements: 33, MaxDurationMs: 1, BatchSize: 2",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  100,
			config: &batcher.Config{
				MaxElements:   33,
				MaxDurationMs: 1,
				BatchSize:     1,
			},
			concurrency: true,
		},
		{
			description: "4 - Collect - Concurrency: false, recordsCnt: 100, MaxElements: 33, MaxDurationMs: 1, BatchSize: 2",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  100,
			config: &batcher.Config{
				MaxElements:   33,
				MaxDurationMs: 1,
				BatchSize:     1,
			},
			concurrency: false,
		},
		{
			description: "5 - Collect - Concurrency: true, recordsCnt: 200, MaxElements: 100, MaxDurationMs: 500, BatchSize: 100",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  200,
			config: &batcher.Config{

				MaxElements:   100,
				MaxDurationMs: 50,
				BatchSize:     100,
			},
			concurrency: true,
		},
		{
			description: "6 - Collect - Concurrency: false, recordsCnt: 200, MaxElements: 100, MaxDurationMs: 500, BatchSize: 100",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  200,
			config: &batcher.Config{

				MaxElements:   100,
				MaxDurationMs: 50,
				BatchSize:     100,
			},
			concurrency: false,
		},
		{
			description: "7 - Collect - Concurrency: true, recordsCnt: 200, MaxElements: 1000, MaxDurationMs: 100, BatchSize: 100",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  200,
			config: &batcher.Config{
				MaxElements:   1000,
				MaxDurationMs: 50,
				BatchSize:     100,
			},
			concurrency: true,
		},
		{
			description: "8 - Collect - Concurrency: false, recordsCnt: 200, MaxElements: 1000, MaxDurationMs: 100, BatchSize: 100",
			table:       "t21",
			initSQL:     aInitSQL,
			recordsCnt:  200,
			config: &batcher.Config{
				MaxElements:   1000,
				MaxDurationMs: 50,
				BatchSize:     100,
			},
			concurrency: false,
		},
	}
	db, err := sql.Open(driver, dsn)
	defer func() { _ = db.Close() }()
	if !assert.Nil(t, err) {
		return
	}

	ctx := context.TODO()

	for _, testCase := range useCases {

		for _, SQL := range testCase.initSQL {
			_, err = db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				return
			}
		}

		var inserter *insert.Service
		inserter, err = insert.New(ctx, db, testCase.table)
		if !assert.Nil(t, err, testCase.description) {
			return
		}

		var collectorSrv *batcher.Service
		collectorSrv, err = batcher.New(ctx, inserter, reflect.TypeOf(&entity{}), testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			return
		}

		// get test records
		testRecords := createTestRecords(testCase.recordsCnt)

		states := make([]*batcher.State, len(testRecords))
		wg := sync.WaitGroup{}
		wg.Add(len(testRecords))

		// running Collect fnc
		for i := range testRecords {
			r := rand.Intn(20)
			time.Sleep(time.Millisecond * time.Duration(r)) // makes goroutines don't start at the same time

			recPtr := testRecords[i]
			fn := func(recPtr *entity, i int) {
				defer wg.Done()
				var state *batcher.State
				state, err = collectorSrv.Collect(recPtr)
				states[i] = state
				assert.Nil(t, err, testCase.description)
			}
			if testCase.concurrency {
				go fn(recPtr, i)
			} else {
				fn(recPtr, i)
			}
		}

		wg.Wait()

		for i, state := range states {
			err = state.Wait()
			if !assert.Nil(t, err, testCase.description) {
				fmt.Printf("error refers to the record nr %d:\n", i)
				toolbox.Dump(testRecords[i])
			}
		}
		onDone, expected, err := loadExpected(ctx, t, testCase, db, testRecords)
		if err != nil {
			return
		}
		sortSlice(testRecords)
		if testCase.concurrency {
			for _, v := range testRecords {
				assert.True(t, v.ID > 0, testCase.description)
			}
			if !assert.EqualValues(t, testRecords, expected, testCase.description) {
				fmt.Println("## testRecords")
				toolbox.Dump(testRecords)
				fmt.Println("## expected")
				toolbox.Dump(expected)
			}
		} else {
			if !assert.EqualValues(t, testRecords, expected, testCase.description) {
				fmt.Println("## testRecords")
				toolbox.Dump(testRecords)
				fmt.Println("## expected")
				toolbox.Dump(expected)
			}
		}
		onDone(err)
	}
}

func sortSlice(a []*entity) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].ID == a[j].ID {
			return a[i].Name < a[j].Name
		}
		return a[i].ID < a[j].ID
	})
}

func loadExpected(ctx context.Context, t *testing.T, testCase *collectTestCase, db *sql.DB, testRecords []*entity) (func(err error), []*entity, error) {
	// checking db contenet
	var err error
	SQL := "SELECT foo_id, foo_name, bar FROM " + testCase.table + " ORDER BY foo_id"
	var rows *sql.Rows
	rows, err = db.QueryContext(ctx, SQL)
	onDone := func(err error) { io.MergeErrorIfNeeded(rows.Close, &err) }
	assert.Nil(t, err, testCase.description)
	recordsFromDB := make([]*entity, len(testRecords))

	i := 0
	for rows.Next() {
		recordsFromDB[i] = &entity{}
		err = rows.Scan(&recordsFromDB[i].ID, &recordsFromDB[i].Name, &recordsFromDB[i].Bar)
		i++
		if !assert.Nil(t, err, testCase.description) {
			return nil, nil, nil
		}
	}
	return onDone, recordsFromDB, err
}

func createTestRecords(count int) []*entity {
	testRecords := make([]*entity, count)

	for i := 0; i < count; i++ {
		testRecords[i] = &entity{
			ID:   0,
			Name: "Lu_" + strconv.Itoa(i),
			Bar:  float64(i),
		}
	}
	return testRecords
}
