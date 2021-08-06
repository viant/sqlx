package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/metadata/option"
	"github.com/viant/sqlx/option"
	"os"

	// "github.com/viant/sqlx/metadata/option"
	"github.com/viant/sqlx/metadata/sink"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"path"
	"testing"
)

type (
	testCase struct {
		skip        bool
		description string
		prepare     []*prepare
	}
	prepare struct {
		dsn    string
		driver string
		sqls   []string
	}
)

func TestAbstractService_DetectVersion(t *testing.T) {
	var ctx = context.Background()
	//var parentDir = ""
	var parentDir = ""
	var testCases = []struct {
		testCase
		expectProduct string
	}{
		{

			testCase: testCase{
				description: "SQLite version detection",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`emp` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
						},
					},
				},
			},
			expectProduct: "SQLite",
		},
	}

	for _, testCase := range testCases {
		db, err := prepareDbs(testCase.prepare)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		meta := New()
		product, err := meta.DetectProduct(ctx, db)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValues(t, testCase.expectProduct, product.Name, testCase.description)
	}
}

func TestAbstractService_Info(t *testing.T) {
	os.Setenv("MYSQL_TEST_HOST", "127.0.0.1:3307")
	mySQLTestHost := os.Getenv("MYSQL_TEST_HOST")
	runMySQLTest := mySQLTestHost != ""
	var ctx = context.Background()
	var parentDir = ""

	var testCases = []struct {
		testCase
		kind    info.Kind
		product *database.Product
		options []option.Option
		sink    Sink
		expect  interface{}
	}{
		{
			testCase: testCase{
				description: "SQLite schemas",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`emp` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
						},
					},
				},
			},
			kind: info.KindSchemas,
			sink: pSchemas([]sink.Schema{}),
			expect: `[
		   				{"Name":"main"}
		   			]`,
		},
		{
			testCase: testCase{
				description: "SQLite schema",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`emp` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
						},
					},
				},
			},
			kind: info.KindSchema,
			sink: pSchemas([]sink.Schema{}),
			expect: `[
		   				{"Name":"main"}
		   			]`,
		},
		{
			testCase: testCase{
				description: "SQLite tables",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`emp` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							"DROP TABLE  IF EXISTS dept",
							"CREATE TABLE `dept` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL)",
						},
					},
				},
			},
			kind: info.KindTables,
			sink: pTables([]sink.Table{}),
			expect: `[
		   	{"@indexBy@":"Name"},
		   	{"Name": "emp", "Owner": "table"},
		   	{"Name": "dept", "Owner": "table"}
		   ]
		   `,
			options: []option.Option{
				option.NewArgs(),
			},
		},

		{
			testCase: testCase{
				description: "SQLite table",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
						},
					},
				},
			},
			kind: info.KindTable,
			sink: pColumns([]sink.Column{}),
			options: []option.Option{
				option.NewArgs("", "", "emp"),
			}, expect: `[
		   	{"Table":"emp","Name":"id","Position":0,"Owner":"INTEGER","Nullable":"0","Key":"PRI"},
		   	{"Table":"emp","Name":"name","Position":1,"Owner":"varchar(255)","Nullable":"1","Default":"NULL","Key":""},
		   	{"Table":"emp","Name":"active","Position":2,"Owner":"tinyint(1)","Nullable":"1","Default":"'1'","Key":""},
		   	{"Table":"emp","Name":"salary","Position":3,"Owner":"decimal(7,2)","Nullable":"1","Default":"NULL","Key":""},
		   	{"Table":"emp","Name":"comments","Position":4,"Owner":"text","Nullable":"1","Key":""},
		   	{"Table":"emp","Name":"last_access_time","Position":5,"Owner":"timestamp","Nullable":"1","Key":""}
		   ]
		   `,
		},

		{
			testCase: testCase{
				description: "SQLite indexes",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							"CREATE UNIQUE INDEX emp_name ON emp (name)",
							"CREATE INDEX emp_active ON emp (active,last_access_time)",
						},
					},
				},
			},
			kind: info.KindIndexes,
			sink: pIndexes([]sink.Index{}),
			options: []option.Option{
				option.NewArgs("", "", "emp"),
			},
			expect: `[
		   				{"Table":"emp","Position":0,"Name":"emp_active","Unique":"0","Origin":"c","Partial":"0", "Columns":"active,last_access_time"},
		   				{"Table":"emp","Position":1,"Name":"emp_name","Unique":"1","Origin":"c","Partial":"0", "Columns":"name"}
		   ]`,
		},

		{
			testCase: testCase{
				description: "SQLite index",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							"CREATE UNIQUE INDEX emp_name ON emp (name)",
							"CREATE INDEX emp_active ON emp (active, last_access_time)",
						},
					},
				},
			},
			kind: info.KindIndex,
			sink: pColumns([]sink.Column{}),
			options: []option.Option{
				option.NewArgs("", "", "emp", "emp_active"),
			},
			expect: `[
		   {"Table":"emp","Name":"active","Position":2,"Key":"1","Descending":"0","Index":"emp_active","IndexPosition":0,"Collation":"BINARY"},
		   {"Table":"emp","Name":"last_access_time","Position":5,"Key":"1","Descending":"0","Index":"emp_active","IndexPosition":1,"Collation":"BINARY"}
		   ]`,
		},

		{
			testCase: testCase{
				description: "SQLite sequences",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							`INSERT INTO emp(name, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments', '2010-05-28T15:36:56.200'),('Sam', 0, 43000, 'test comments', '2010-05-28T15:36:56.200')`,
						},
					},
				},
			},
			kind: info.KindSequences,
			sink: pSequences([]sink.Sequence{}),
			options: []option.Option{
				option.NewArgs(),
			},
			expect: `[{"Name":"emp","Value":2}]`,
		},
		{
			testCase: testCase{
				description: "SQLite sequence",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							`INSERT INTO emp(name, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments', '2010-05-28T15:36:56.200'),('Sam', 0, 43000, 'test comments', '2010-05-28T15:36:56.200')`,
						},
					},
				},
			},
			kind: info.KindSequences,
			sink: pSequences([]sink.Sequence{}),
			options: []option.Option{
				option.NewArgs("", "", "emp"),
			},
			expect: `[{"Name":"emp","Value":2}]`,
		},

		{
			testCase: testCase{
				description: "SQLite primary key",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
							`INSERT INTO emp(name, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments', '2010-05-28T15:36:56.200'),('Sam', 0, 43000, 'test comments', '2010-05-28T15:36:56.200')`,
						},
					},
				},
			},
			kind: info.KindPrimaryKeys,
			sink: pKeys([]sink.Key{}),
			options: []option.Option{
				option.NewArgs("", "", "emp"),
			},
			expect: `[{"Name":"emp_pk","Owner":"PRIMARY KEY","Table":"emp","Position":0,"Column":"id"}]`,
		},
		{
			testCase: testCase{
				description: "SQLite foreign keys",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS artist",
							"CREATE TABLE artist(artistid   INTEGER PRIMARY KEY, artistname  TEXT);",
							"DROP TABLE  IF EXISTS track",
							"CREATE TABLE track( trackid     INTEGER,  trackname   TEXT,  trackartist INTEGER, FOREIGN KEY(trackartist) REFERENCES artist(artistid))",
						},
					},
				},
			},
			kind: info.KindForeignKeys,
			sink: pKeys([]sink.Key{}),
			options: []option.Option{
				option.NewArgs("", "", "track"),
			},
			expect: `[{"Name":"track_artist_fk","Table":"track","Position":0,"Column":"trackartist","ReferenceTable":"artist","ReferenceColumn":"artistid","ConstrainPosition":0,"OnUpdate":"NO ACTION","OnDelete":"NO ACTION","OnMatch":"NONE"}]`,
		},
		{
			testCase: testCase{
				description: "SQLite functions",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS emp",
							"CREATE TABLE `emp` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`name` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
						},
					},
				},
			},
			kind: info.KindFunctions,
			sink: pFunction([]sink.Function{}),
			options: []option.Option{
				option.NewArgs("", "", "rank"),
			},
			expect: `[{
		   		"Name": "rank",
		   		"Body": "",
		   		"DataType": "NUMERIC",
		   		"Owner": "NATIVE",
		   		"Charset": "utf8",
		   		"Deterministic": "NO"}]`,
		},

		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL schemas",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
					}},
			},
			kind: info.KindSchema,
			sink: pSchemas([]sink.Schema{}),
			options: []option.Option{
				option.NewArgs("", "mysql"),
			},
			expect: `[{"Name": "mysql","Path": "","Sequence": 0}]`,
		},
		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL table",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`CREATE TABLE  IF NOT EXISTS  mydb.emp(id MEDIUMINT NOT NULL AUTO_INCREMENT, name CHAR(30) NOT NULL, PRIMARY KEY (id))`,
						},
					}},
			},
			kind: info.KindTable,
			sink: pColumns([]sink.Column{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "emp"),
			},
			expect: `[
{"Schema":"mydb","Table":"emp","Name":"id","Position":1,"Owner":"mediumint","Precision":7,"Scale":0,"Nullable":"NO","Key":"PRI"},
{"Schema":"mydb","Table":"emp","Name":"name","Position":2,"Owner":"char","Length":30,"Nullable":"NO"}
]
`,
		},

		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL sequence",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`CREATE TABLE  IF NOT EXISTS  mydb.emp(id MEDIUMINT NOT NULL AUTO_INCREMENT, name CHAR(30) NOT NULL, PRIMARY KEY (id))`,
						},
					}},
			},
			kind: info.KindSequences,
			sink: pSequences([]sink.Sequence{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "emp"),
			},
			expect: `[{"Schema":"mydb","Name":"emp","Value":1,"DataType":"mediumint(9)"}]`,
		},
		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL indexes",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`CREATE TABLE  IF NOT EXISTS  mydb.emp(id MEDIUMINT NOT NULL AUTO_INCREMENT, name CHAR(30) NOT NULL, PRIMARY KEY (id))`,
						},
					}},
			},
			kind: info.KindIndexes,
			sink: pIndexes([]sink.Index{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "emp"),
			},
			expect: `[
	{"Table":"emp","Owner":"BTREE","TableSchema":"mydb","Schema":"mydb","Position":0,"Name":"PRIMARY","Columns":"id"}
]
`,
		},
		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL index",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`CREATE TABLE  IF NOT EXISTS  mydb.emp(id MEDIUMINT NOT NULL AUTO_INCREMENT, name CHAR(30) NOT NULL, PRIMARY KEY (id))`,
						},
					}},
			},
			kind: info.KindIndex,
			sink: pColumns([]sink.Column{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "emp", "PRIMARY"),
			},
			expect: `[
{"Schema":"mydb","Table":"emp","Name":"id","Position":0,"Index":"PRIMARY","IndexPosition":1}
]`,
		},

		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL Primary Key",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`CREATE TABLE  IF NOT EXISTS  mydb.emp(id MEDIUMINT NOT NULL AUTO_INCREMENT, name CHAR(30) NOT NULL, PRIMARY KEY (id))`,
						},
					}},
			},
			kind: info.KindPrimaryKeys,
			sink: pKeys([]sink.Key{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "emp"),
			},
			expect: `[
{"Name":"PRIMARY","Owner":"PRIMARY KEY","Schema":"mydb","Table":"emp","Position":0,"Column":"id"}
]`,
		},

		{
			testCase: testCase{
				skip:        !runMySQLTest,
				description: "MySQL index",
				prepare: []*prepare{
					{
						driver: "mysql",
						dsn:    fmt.Sprintf("root:dev@tcp(%s)/mysql?parseTime=true", mySQLTestHost),
						sqls: []string{
							`CREATE DATABASE IF NOT EXISTS mydb`,
							`DROP TABLE IF EXISTS mydb.shirt`,
							`DROP TABLE IF EXISTS mydb.person`,
							`CREATE TABLE mydb.person (
			    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
			    name CHAR(60) NOT NULL,
			    PRIMARY KEY (id)
			)`,
							`CREATE TABLE mydb.shirt (
			    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
			    style ENUM('t-shirt', 'polo', 'dress') NOT NULL,
			    color ENUM('red', 'blue', 'orange', 'white', 'black') NOT NULL,
			    owner SMALLINT UNSIGNED NOT NULL,
			    PRIMARY KEY (id),
  				CONSTRAINT person_fk FOREIGN KEY (owner)
    			REFERENCES person(id	)
			)`,
						},
					}},
			},
			kind: info.KindForeignKeys,
			sink: pKeys([]sink.Key{}),
			options: []option.Option{
				option.NewArgs("", "mydb", "shirt"),
			},
			expect: `[{"Name":"person_fk","Owner":"FOREIGN KEY","Table":"shirt","Position":0,"Column":"owner","ReferenceTable":"person","ReferenceColumn":"id"}]`,
		},
	}

	for _, testCase := range testCases {
		fmt.Printf("===%v\n", testCase.description)
		if testCase.skip {
			t.Skipf("skipped " + testCase.description)
			continue
		}
		db, err := prepareDbs(testCase.prepare)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		meta := New()
		actual := testCase.sink
		err = meta.Info(ctx, db, nil, testCase.kind, actual, testCase.options...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		//if !assertly.AssertValues(t, testCase.expect, actual, testCase.description) {
		//	toolbox.Dump(actual)
		//	toolbox.DumpIndent(actual, true)
		//}
	}

}

func TestAbstractService_Execute(t *testing.T) {

	//	runMySQLTest := true
	var ctx = context.Background()
	var parentDir = ""

	var testCases = []struct {
		testCase
		kind    info.Kind
		product *database.Product
		options []option.Option
		sink    Sink
	}{
		{
			testCase: testCase{
				description: "SQLite schemas",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS artist",
							"CREATE TABLE artist(artistid   INTEGER PRIMARY KEY, artistname  TEXT);",
							"DROP TABLE  IF EXISTS track",
							"CREATE TABLE track( trackid     INTEGER,  trackname   TEXT,  trackartist INTEGER, FOREIGN KEY(trackartist) REFERENCES artist(artistid))",
						},
					},
				},
			},
			kind: info.KindForeignKeysCheckOff,
			sink: pSchemas([]sink.Schema{}),
		},
		{
			testCase: testCase{
				description: "SQLite schemas",
				prepare: []*prepare{
					{
						driver: "sqlite3",
						dsn:    path.Join(parentDir, "/tmp/mydb.db"),
						sqls: []string{
							"DROP TABLE  IF EXISTS artist",
							"CREATE TABLE artist(artistid   INTEGER PRIMARY KEY, artistname  TEXT);",
							"DROP TABLE  IF EXISTS track",
							"CREATE TABLE track( trackid     INTEGER,  trackname   TEXT,  trackartist INTEGER, FOREIGN KEY(trackartist) REFERENCES artist(artistid))",
						},
					},
				},
			},
			kind: info.KindForeignKeysCheckOn,
			sink: pSchemas([]sink.Schema{}),
		},
	}

	for _, testCase := range testCases {
		if testCase.skip {
			t.Skipf("skipped " + testCase.description)
			continue
		}
		db, err := prepareDbs(testCase.prepare)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		meta := New()
		_, err = meta.Execute(ctx, db, testCase.kind, option.NewArgs("", "", ""))
		assert.Nil(t, err, testCase.description)
	}

}

func prepareDbs(preps []*prepare) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for _, prep := range preps {
		if db, err = prepareDb(prep); err != nil {
			return db, err
		}
	}
	return db, nil
}

func prepareDb(prep *prepare) (*sql.DB, error) {
	if prep == nil {
		return nil, nil
	}
	ctx := context.Background()
	fs := afs.New()
	if ok, _ := fs.Exists(ctx, prep.dsn); ok {
		fs.Delete(ctx, prep.dsn)
	}

	db, err := sql.Open(prep.driver, prep.dsn)
	if err != nil {
		return nil, err
	}
	for _, SQL := range prep.sqls {
		_, err = db.Exec(SQL)
		if err != nil {
			return nil, err
		}
	}
	return db, err
}

func pStrings(s []string) *[]string {
	return &s
}

func pSchemas(s []sink.Schema) *[]sink.Schema {
	return &s
}

func pTables(s []sink.Table) *[]sink.Table {
	return &s
}

func pColumns(s []sink.Column) *[]sink.Column {
	return &s
}

func pIndexes(s []sink.Index) *[]sink.Index {
	return &s
}

func pFunction(s []sink.Function) *[]sink.Function {
	return &s
}

func pSequences(s []sink.Sequence) *[]sink.Sequence {
	return &s
}

func pKeys(s []sink.Key) *[]sink.Key {
	return &s
}
