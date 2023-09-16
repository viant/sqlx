package read_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	option2 "github.com/viant/afs/option"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const uint32Max uint32 = 4294967295

type recorder struct {
	scannedValues [][]interface{}
	addedValues   [][]interface{}
}

func (r *recorder) AddValues(values []interface{}) {
	r.addedValues = append(r.addedValues, values)
}

func (r *recorder) ScanValues(values []interface{}) {
	valuesCopy := make([]interface{}, len(values))
	copy(valuesCopy, values)
	r.scannedValues = append(r.scannedValues, valuesCopy)
}

type (
	usecase struct {
		description     string
		query           string
		driver          string
		dsn             string
		newRow          func() interface{}
		params          []interface{}
		expect          string
		initSQL         []string
		hasMapperError  bool
		resolver        *io.Resolver
		expectResolved  string
		args            []interface{}
		expectedAdded   string
		expectedScanned string
		removeCache     bool
		disableCache    *bool
		rowMapperCache  *read.MapperCache
		cacheConfig     *cacheConfig
		cacheWarmup     *cacheWarmup
		matcher         *cache.ParmetrizedQuery
	}

	cacheWarmup struct {
		column string
		SQL    string
		args   []interface{}
	}

	cacheConfig struct {
		location  string
		duration  time.Duration
		signature string
		cacheType string
	}
)

// TODO: Fix policies, specially when it comes to expiration time
// TODO: Fix test cases to make them less vulnerable against the time.
func TestReader_ReadAll(t *testing.T) {
	cache.Now = func() time.Time {
		parse, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Feb 4, 2014 at 6:05pm (PST)")
		return parse
	}
	testLocation := toolbox.CallerDirectory(3)
	cacheLocation := path.Join(testLocation, "testdata", "cache")

	type fooCase1 struct {
		Id   int
		Name string
	}

	type fooCase2 struct {
		Id   int    `sqlx:"foo_id"`
		Name string `sqlx:"foo_name"`
		Desc string `sqlx:"-"`
		Bar  float64
	}

	type case3FooID struct {
		Id   int `sqlx:"foo_id"`
		Desc string
	}

	type Case3FooName struct {
		Name string
	}

	type case3Wrapper struct {
		*case3FooID
		Case3FooName `sqlx:"ns=foo"`
	}

	type Boo struct {
		Val int `sqlx:"name=id"`
	}

	type Foo struct {
		Id    int
		Name  string
		Price float64
	}

	type fooWrapper struct {
		Boo
		*Foo
	}

	var useCases = []*usecase{
		{
			description: "Reading slice input   ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id , name  from t1 order by 1  ",
			newRow: func() interface{} {
				return make([]interface{}, 2)
			},
			expect: `[[1,"John"],[2,"Bruce"]]`,
		},
		{
			description: "Reading vanilla struct",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select * from t1 order by id ",
			newRow: func() interface{} {
				return &fooCase1{}
			},
			expect: `[{"Id":1,"Name":"John"},{"Id":2,"Name":"Bruce"}]`,
		},
		{
			description: "Reading struct with tags  ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id as foo_id, name as foo_name from t1 order by 1 ",
			newRow: func() interface{} {
				return &fooCase2{}
			},
			expect: `[{"Id":1,"Name":"John","Desc":"","Bar":0},{"Id":2,"Name":"Bruce","Desc":"","Bar":0}]`,
		},
		{
			description: "Reading map input   ",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			query: "select id , name  from t1 order by 1  ",
			newRow: func() interface{} {
				return make(map[string]interface{})
			},
			expect: `[{"id":1,"name":"John"},{"id":2,"name":"Bruce"}]`,
		},
		{
			description: "Complex struct mapper",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t3 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT)",
				"delete from t3",
				"insert into t3 values(1, \"John\", \"desc1\")",
				"insert into t3 values(2, \"Bruce\", \"desc2\")",
			},
			query: "select foo_id , foo_name, desc  from t3 order by 1  ",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			expect: `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
		},
		{
			description: "Complex struct mapper with unresolved handelr",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t4 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT)",
				"delete from t4",
				"insert into t4 values(1, \"John\", \"desc1\")",
				"insert into t4 values(2, \"Bruce\", \"desc2\")",
			},
			query: "SELECT foo_id , foo_name, desc, '123' AS unk  FROM t4 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			hasMapperError: true,
		},
		{
			description: "Complex struct mapper with unmappd handelr",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t4 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t4",
				"insert into t4 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t4 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t4 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["101","102"]`,
		},
		{
			description: "Cache",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t5 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t5",
				"insert into t5 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t5 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t5 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["101","102"]`,
			cacheConfig: &cacheConfig{
				location:  cacheLocation,
				duration:  time.Duration(10000) * time.Minute,
				signature: "events",
			},
			expectedScanned: `[[1,"John","desc1","101"],[2,"Bruce","desc2","102"]]`,
		},
		{
			description: "Cache with args",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t5 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t5",
				"insert into t5 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t5 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t5 WHERE foo_id = ? ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["102"]`,
			cacheConfig: &cacheConfig{
				location: cacheLocation, duration: time.Duration(10000) * time.Minute,
			},
			args:          []interface{}{2},
			expectedAdded: `[[2,"Bruce","desc2","102"]]`,
			removeCache:   true,
		},
		{
			description: "RowMapper cache",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t4 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t4",
				"insert into t4 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t4 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t4 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["101","102"]`,
			rowMapperCache: read.NewMapperCache(1024),
		},
		{
			description: "Disabled cache",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t6 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t6",
				"insert into t6 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t6 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t6 ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			disableCache:   boolPtr(true),
			expectResolved: `["101","102"]`,
		},
		{
			description: "Embedded structs with same fields",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t7 (id INTEGER PRIMARY KEY, name TEXT, price NUMERIC)",
				"delete from t7",
				"insert into t7 values(1, \"John\", 101)",
				"insert into t7 values(2, \"Bruce\", 102)",
			},
			query: "SELECT id , name, price  FROM t7 ORDER BY 1",
			newRow: func() interface{} {
				return &fooWrapper{}
			},
			expect: `[{"Val":1,"Id":0,"Name":"John","Price":101},{"Val":2,"Id":0,"Name":"Bruce","Price":102}]`,
		},
		{
			description: "Aerospike cache write",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t8 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t8",
				"insert into t8 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t8 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t8 WHERE foo_id = ? ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved: `["102"]`,
			args:           []interface{}{2},
			expectedAdded:  `[[2,"Bruce","desc2","102"]]`,
			removeCache:    true,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
		},
		{
			description: "Aerospike cache read",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t9 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t9",
				"insert into t9 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t9 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t9 WHERE foo_id = ? ORDER BY 1",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			cacheWarmup: &cacheWarmup{
				SQL:  "SELECT foo_id , foo_name, desc,  unk  FROM t9 WHERE foo_id = ? ORDER BY 1",
				args: []interface{}{2},
			},
			resolver:        io.NewResolver(),
			expect:          `[{"Id":2,"Desc":"desc2","Name":"Bruce"}]`,
			expectResolved:  `["102"]`,
			args:            []interface{}{2},
			expectedScanned: `[[2,"Bruce","desc2","102"]]`,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
		},
		{
			description: "Aerospike smart cache read",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t10 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t10",
				"insert into t10 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t10 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t10 ORDER BY 1 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:        io.NewResolver(),
			expectedScanned: `[[2,"Bruce","desc2","102"],[1,"John","desc1","101"]]`,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"},{"Id":1,"Desc":"desc1","Name":"John"}]`,
			expectResolved: `["102","101"]`,
			cacheWarmup: &cacheWarmup{
				column: "foo_id",
				SQL:    "SELECT * FROM t10 ORDER BY 1 DESC",
			},
			matcher: &cache.ParmetrizedQuery{
				SQL:  "SELECT * FROM t10 ORDER BY 1 DESC",
				Args: []interface{}{},
				By:   "foo_id",
				In:   []interface{}{1, 2},
			},
		},
		{
			description: "Aerospike cache with record pagination",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t11 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t11",
				`insert into t11 values(1, "John", "desc1", "101")`,
				`insert into t11 values(2, "Bruce", "desc2", "102")`,
				`insert into t11 values(3, "Name - 1", "desc3", "102")`,
				`insert into t11 values(4, "Name - 2", "desc4", "102")`,
				`insert into t11 values(5, "Name - 3", "desc5", "101")`,
			},
			query: "SELECT * FROM t11 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:        io.NewResolver(),
			expectedScanned: `[[1,"John","desc1","101"],[5,"Name - 3","desc5","101"],[2,"Bruce","desc2","102"],[3,"Name - 1","desc3","102"]]`,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"},{"Id":5,"Desc":"desc5","Name":"Name - 3"},{"Id":2,"Desc":"desc2","Name":"Bruce"},{"Id":3,"Desc":"desc3","Name":"Name - 1"}]`,
			expectResolved: `["102","102","101","101"]`,
			cacheWarmup: &cacheWarmup{
				column: "unk",
				SQL:    "SELECT * FROM t11 ORDER BY 4 DESC",
			},
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t11 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 0,
				Limit:  2,
			},
		},
		{
			description: "Database record pagination",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t12 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t12",
				`insert into t12 values(1, "John", "desc1", "101")`,
				`insert into t12 values(2, "Bruce", "desc2", "102")`,
				`insert into t12 values(3, "Name - 1", "desc3", "102")`,
				`insert into t12 values(4, "Name - 2", "desc4", "102")`,
				`insert into t12 values(5, "Name - 3", "desc5", "101")`,
			},
			query: "SELECT * FROM t12 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":3,"Desc":"desc3","Name":"Name - 1"},{"Id":4,"Desc":"desc4","Name":"Name - 2"},{"Id":5,"Desc":"desc5","Name":"Name - 3"}]`,
			expectResolved: `["102","102","101"]`,
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t12 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 1,
				Limit:  2,
			},
		},
		{
			description: "Database record pagination | offset > len(table)",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t13 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t13",
				`insert into t13 values(1, "John", "desc1", "101")`,
				`insert into t13 values(2, "Bruce", "desc2", "102")`,
				`insert into t13 values(3, "Name - 1", "desc3", "102")`,
				`insert into t13 values(4, "Name - 2", "desc4", "102")`,
				`insert into t13 values(5, "Name - 3", "desc5", "101")`,
			},
			query: "SELECT * FROM t13 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[]`,
			expectResolved: `[]`,
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t13 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 10,
				Limit:  2,
			},
		},
		{
			description: "Aerospike cache with record pagination | offset > len(table)",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t14 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t14",
				`insert into t14 values(1, "John", "desc1", "101")`,
				`insert into t14 values(2, "Bruce", "desc2", "102")`,
				`insert into t14 values(3, "Name - 1", "desc3", "102")`,
				`insert into t14 values(4, "Name - 2", "desc4", "102")`,
				`insert into t14 values(5, "Name - 3", "desc5", "101")`,
			},
			query: "SELECT * FROM t14 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:        io.NewResolver(),
			expectedScanned: `[]`,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
			expect:         `[]`,
			expectResolved: `[]`,
			cacheWarmup: &cacheWarmup{
				column: "unk",
				SQL:    "SELECT * FROM t14 ORDER BY 4 DESC",
			},
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t14 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 100,
				Limit:  2,
			},
		},
		{
			description: "Database record pagination | limit: 0, offset: 0",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t13 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t13",
				`insert into t13 values(1, "John", "desc1", "101")`,
				`insert into t13 values(2, "Bruce", "desc2", "102")`,
				`insert into t13 values(3, "Name - 1", "desc3", "102")`,
				`insert into t13 values(4, "Name - 2", "desc4", "102")`,
				`insert into t13 values(5, "Name - 3", "desc5", "101")`,
				`insert into t13 values(6, "Name - 4", "desc6", "101")`,
			},
			query: "SELECT * FROM t13 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"},{"Id":3,"Desc":"desc3","Name":"Name - 1"},{"Id":4,"Desc":"desc4","Name":"Name - 2"},{"Id":1,"Desc":"desc1","Name":"John"},{"Id":5,"Desc":"desc5","Name":"Name - 3"},{"Id":6,"Desc":"desc6","Name":"Name - 4"}]`,
			expectResolved: `["102","102","102","101","101","101"]`,
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t13 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 0,
				Limit:  0,
			},
		},
		{
			description: "Database record pagination | limit: 2, pagination: 0",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t14 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t14",
				`insert into t14 values(1, "John", "desc1", "101")`,
				`insert into t14 values(2, "Bruce", "desc2", "102")`,
				`insert into t14 values(3, "Name - 1", "desc3", "102")`,
				`insert into t14 values(4, "Name - 2", "desc4", "102")`,
				`insert into t14 values(5, "Name - 3", "desc5", "101")`,
				`insert into t14 values(6, "Name - 4", "desc6", "101")`,
			},
			query: "SELECT * FROM t14 ORDER BY 4 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:       io.NewResolver(),
			expect:         `[{"Id":2,"Desc":"desc2","Name":"Bruce"},{"Id":3,"Desc":"desc3","Name":"Name - 1"},{"Id":1,"Desc":"desc1","Name":"John"},{"Id":5,"Desc":"desc5","Name":"Name - 3"}]`,
			expectResolved: `["102","102","101","101"]`,
			matcher: &cache.ParmetrizedQuery{
				SQL:    "SELECT * FROM t14 ORDER BY 4 DESC",
				Args:   []interface{}{},
				By:     "unk",
				In:     []interface{}{"101", "102"},
				Offset: 0,
				Limit:  2,
			},
		},
		{
			description: "Aerospike smart cache read with pagination",
			driver:      "sqlite3",
			dsn:         "/tmp/sqllite.db",
			initSQL: []string{
				"CREATE TABLE IF NOT EXISTS t15 (foo_id INTEGER PRIMARY KEY, foo_name TEXT, desc TEXT, unk TEXT)",
				"delete from t15",
				"insert into t15 values(0, \"John - 0\", \"desc0\", \"100\")",
				"insert into t15 values(1, \"John\", \"desc1\", \"101\")",
				"insert into t15 values(2, \"Bruce\", \"desc2\", \"102\")",
			},
			query: "SELECT foo_id , foo_name, desc,  unk  FROM t15 ORDER BY 1 DESC",
			newRow: func() interface{} {
				return &case3Wrapper{}
			},
			resolver:        io.NewResolver(),
			expectedScanned: `[[1,"John","desc1","101"]]`,
			cacheConfig: &cacheConfig{
				cacheType: "aerospike",
			},
			expect:         `[{"Id":1,"Desc":"desc1","Name":"John"}]`,
			expectResolved: `["101"]`,
			cacheWarmup: &cacheWarmup{
				column: "foo_id",
				SQL:    "SELECT * FROM t15 ORDER BY 1 DESC",
			},
			matcher: &cache.ParmetrizedQuery{
				SQL:   "SELECT * FROM t15 ORDER BY 1 DESC",
				Args:  []interface{}{},
				By:    "foo_id",
				In:    []interface{}{1},
				Limit: 1,
			},
		},
	}

outer:
	//for i, testCase := range useCases[len(useCases)-1:] {
	for i, testCase := range useCases {
		if testCase.matcher != nil {
			testCase.matcher.OnSkip = testCase.resolver.OnSkip
		}

		fmt.Printf("Running testcase: %v | %v\n", i, testCase.description)
		os.RemoveAll(testCase.dsn)
		ctx := context.Background()

		db, err := sql.Open(testCase.driver, testCase.dsn)
		if !assert.Nil(t, err, testCase.description) {
			log.Panic(err)
		}

		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				continue outer
			}
		}

		aCache, aRecorder, err := getCacheWithRecorder(db, testCase)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		var options = make([]option.Option, 0)
		if testCase.resolver != nil {
			options = append(options, testCase.resolver.Resolve)
		}

		if aCache != nil {
			options = append(options, aCache)
		}

		if testCase.rowMapperCache != nil {
			options = append(options, testCase.rowMapperCache)
		}

		if testCase.disableCache != nil {
			options = append(options, read.DisableMapperCache(*testCase.disableCache))
		}

		if testCase.matcher != nil {
			options = append(options, testCase.matcher)
		}

		reader, err := read.New(ctx, db, testCase.query, testCase.newRow, options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		dbRequests := 1
		if testCase.rowMapperCache != nil {
			dbRequests = 2
		}

		for j := 0; j < dbRequests; j++ {
			if !testQueryAll(t, reader, testCase, j, aRecorder, aCache) {
				continue outer
			}
		}
	}
}

func getCacheWithRecorder(db *sql.DB, testCase *usecase) (cache.Cache, *recorder, error) {
	config := testCase.cacheConfig
	if config == nil {
		return nil, nil, nil
	}

	aRecorder := &recorder{}
	aCache, err := getCache(aRecorder, config)

	warmup := testCase.cacheWarmup
	if warmup != nil {
		if _, err = aCache.IndexBy(context.TODO(), db, warmup.column, warmup.SQL, warmup.args); err != nil {
			return nil, nil, err
		}
	}

	return aCache, aRecorder, err
}

func getCache(aRecorder *recorder, config *cacheConfig) (cache.Cache, error) {
	if config.cacheType != "aerospike" {
		return afs.NewCache(config.location, config.duration, config.signature, option2.NewStream(64*1024*1024, 64*1024), aRecorder)
	}

	client, err := as.NewClient("127.0.0.1", 3000)
	if err != nil {
		return nil, err
	}

	return aerospike.New("test", "aerospike", client, 0, aRecorder)
}

func boolPtr(b bool) *bool {
	return &b
}

func testQueryAll(t *testing.T, reader *read.Reader, testCase *usecase, index int, aRecorder *recorder, aCache cache.Cache) bool {
	var actual = make([]interface{}, 0)
	err := reader.QueryAll(context.TODO(), func(row interface{}) error {
		actual = append(actual, row)
		return nil
	}, testCase.args...)

	if testCase.hasMapperError {
		assert.NotNil(t, t, err, testCase.description)
		return false
	}

	if !assert.Nil(t, err, testCase.description) {
		return false
	}

	actualJSON, _ := json.Marshal(actual)
	if !slicesEqualIgnoreOrder(t, testCase.description, testCase.expect, string(actualJSON)) {
		return false
	}

	if testCase.resolver != nil {
		actualJSON, _ := json.Marshal(testCase.resolver.Data(index))
		if !slicesEqualIgnoreOrder(t, testCase.description, testCase.expectResolved, string(actualJSON)) {
			fmt.Println(actualJSON)
			return false
		}
	}

	if aRecorder != nil {
		if testCase.expectedScanned != "" {
			marshal, _ := json.Marshal(aRecorder.scannedValues)
			slicesEqualIgnoreOrder(t, testCase.description, testCase.expectedScanned, string(marshal))
		}

		if testCase.expectedAdded != "" {
			marshal, _ := json.Marshal(aRecorder.addedValues)
			slicesEqualIgnoreOrder(t, testCase.description, testCase.expectedAdded, string(marshal))
		}
	}

	if aCache != nil {
		cacheEntry, err := aCache.Get(context.TODO(), testCase.query, testCase.args, testCase.matcher)
		assert.Nil(t, err, testCase.description)

		if cacheEntry == nil {
			return true
		}

		assert.True(t, cacheEntry.Has(), testCase.description)
		argsMarshal, _ := json.Marshal(testCase.args)
		assert.Equal(t, argsMarshal, cacheEntry.Meta.Args, testCase.description)
		assert.Equal(t, testCase.query, cacheEntry.Meta.SQL, testCase.description)
		if testCase.removeCache {
			assert.Nil(t, aCache.Delete(context.TODO(), cacheEntry), testCase.description)
		}
	}
	return true
}

func slicesEqualIgnoreOrder(t *testing.T, description string, expected, actual string) bool {
	var xSlice []interface{}
	if len(expected) != 0 && !assert.Nil(t, json.Unmarshal([]byte(expected), &xSlice), description) {
		return false
	}

	var ySlice []interface{}
	if len(actual) != 0 && !assert.Nil(t, json.Unmarshal([]byte(actual), &ySlice), description) {
		return false
	}

	if len(xSlice) != len(ySlice) {
		return assert.Equal(t, xSlice, ySlice, description)
	}

outer:
	for _, xValue := range xSlice {
		for j, yValue := range ySlice {
			if yValue == nil {
				continue
			}

			xValueMarshal, _ := json.Marshal(xValue)
			yValueMarshal, _ := json.Marshal(yValue)

			if string(xValueMarshal) == string(yValueMarshal) {
				ySlice[j] = nil
				continue outer
			}
		}

		return assert.Nil(t, fmt.Errorf("not found equal object %v in %v", xValue, ySlice), description)
	}

	return true
}

func BenchmarkStructMapper(b *testing.B) {
	db, err := sql.Open("sqlite3", "/tmp/read-bench.db")
	if !assert.Nil(b, err) {
		return
	}

	_, err = db.Exec("DROP TABLE IF EXISTS foos;")
	if !assert.Nil(b, err) {
		return
	}

	_, err = db.Exec(`CREATE TABLE foos(
    	ID Integer Primary Key, 
		Name TEXT,
		Price Numeric,
		InsertedAt INTEGER,
		UpdatedAt INTEGER,
		ModifiedBy TEXT,
		StringPtr TEXT,
		IntPtr INTEGER,
		Float64Ptr NUMERIC,
		Int64Ptr INTEGER,
		BoolPtr INTEGER 
		)`)

	if !assert.Nil(b, err) {
		return
	}

	tx, err := db.BeginTx(context.TODO(), nil)
	if !assert.Nil(b, err) {
		return
	}

	dataSize := 1000
	for i := 0; i < dataSize; i++ {
		_, err = tx.Exec(`INSERT INTO foos (ID, Name, Price, InsertedAt, UpdatedAt, ModifiedBy) VALUES (
			?, ?, ?, ?, ?, ?
			)`, i, "Foo Name "+strconv.Itoa(i), float64(i)+0.5, time.Now().UnixNano(), time.Now().UnixNano(), "abc")
		if !assert.Nil(b, err) {
			_ = tx.Rollback()
			return
		}
	}

	if !assert.Nil(b, tx.Commit()) {
		return
	}

	type Audit struct {
		InsertedAt int
		UpdatedAt  int
		ModifiedBy string
	}

	type Optional struct {
		StringPtr  *string
		IntPtr     *int
		Float64Ptr *float64
		Int64Ptr   *int64
		BoolPtr    *bool
	}

	type Foo struct {
		ID    int
		Name  string
		Price float64
		Audit
		Optional
	}

	b.Run("Mapper creation", func(b *testing.B) {
		rows, err := db.Query("SELECT * FROM foos WHERE 1 = 2")
		if !assert.Nil(b, err) {
			return
		}

		types, err := rows.ColumnTypes()
		if !assert.Nil(b, err) {
			return
		}

		columns := io.TypesToColumns(types)
		fooPtrType := reflect.TypeOf(&Foo{})

		b.Run("Without cache", func(b *testing.B) {
			var err error
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err = read.NewStructMapper(columns, fooPtrType, "", io.NewResolver().Resolve, read.DisableMapperCache(true))
			}
			assert.Nil(b, err)
		})

		b.Run("With cache", func(b *testing.B) {
			var err error
			mapperCache := read.NewMapperCache(1024)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err = read.NewStructMapper(columns, fooPtrType, "", io.NewResolver().Resolve, mapperCache)
			}
			assert.Nil(b, err)
		})
	})

	b.Run("Without mapper cache", func(b *testing.B) {
		reader, err := read.New(context.TODO(), db, "SELECT * FROM foos", func() interface{} {
			return &Foo{}
		})
		if !assert.Nil(b, err) {
			return
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			counter := 0
			err = reader.QueryAll(context.TODO(), func(row interface{}) error {
				counter++
				return nil
			})
			assert.Nil(b, err)
			assert.Equal(b, dataSize, counter)
		}
	})

	b.Run("With mapper cache", func(b *testing.B) {
		mapperCache := read.NewMapperCache(1024)
		reader, err := read.New(context.TODO(), db, "SELECT * FROM foos", func() interface{} {
			return &Foo{}
		}, mapperCache)

		if !assert.Nil(b, err) {
			return
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			counter := 0
			err = reader.QueryAll(context.TODO(), func(row interface{}) error {
				counter++
				return nil
			})
			assert.Nil(b, err)
			assert.Equal(b, dataSize, counter)
		}
	})

	b.Run("With mapper cache and file data cache", func(b *testing.B) {
		mapperCache := read.NewMapperCache(1024)
		dataCache, err := afs.NewCache("/tmp/cache", time.Duration(1)*time.Minute, "", option2.NewStream(64*1024*1024, 64*1024))
		if !assert.Nil(b, err) {
			return
		}

		cacheReader, err := read.New(context.TODO(), db, "SELECT * FROM foos", func() interface{} {
			return &Foo{}
		}, mapperCache, dataCache)

		if !assert.Nil(b, err) {
			return
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			counter := 0
			err = cacheReader.QueryAll(context.TODO(), func(row interface{}) error {
				counter++
				return nil
			})
			assert.Nil(b, err)
			assert.Equal(b, dataSize, counter)
		}
	})

	b.Run("With mapper cache and memory data cache", func(b *testing.B) {
		mapperCache := read.NewMapperCache(1024)
		dataCache, err := afs.NewCache("mem:///tmp/cache", time.Duration(1)*time.Minute, "", option2.NewStream(64*1024*1024, 64*1024))
		if !assert.Nil(b, err) {
			return
		}

		cacheReader, err := read.New(context.TODO(), db, "SELECT * FROM foos", func() interface{} {
			return &Foo{}
		}, mapperCache, dataCache)

		if !assert.Nil(b, err) {
			return
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			counter := 0
			err = cacheReader.QueryAll(context.TODO(), func(row interface{}) error {
				counter++
				return nil
			})
			assert.Nil(b, err)
			assert.Equal(b, dataSize, counter)
		}
	})

	b.Run("With mapper cache and aerospike data cache", func(b *testing.B) {
		mapperCache := read.NewMapperCache(1024)
		client, err := as.NewClient("127.0.0.1", 3000)
		if !assert.Nil(b, err) {
			return
		}

		dataCache, err := aerospike.New("test", "aerospike", client, 0)
		if !assert.Nil(b, err) {
			return
		}

		cacheReader, err := read.New(context.TODO(), db, "SELECT * FROM foos", func() interface{} {
			return &Foo{}
		}, mapperCache, dataCache)

		if !assert.Nil(b, err) {
			return
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			counter := 0
			err = cacheReader.QueryAll(context.TODO(), func(row interface{}) error {
				counter++
				return nil
			})
			assert.Nil(b, err)
			assert.Equal(b, dataSize, counter)
		}
	})

}
