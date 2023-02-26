# sqlx - Comprehensive SQL Extensions For Go


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
  - [Dictionary metadata](#dictionary-metadata)
  - [Reader Service](#reader-service)
  - [Inserter Service](#inserter-service)
  - [Updater Service](#updater-service)
  - [Merger Service](#merger-service)
  - [Loader Service](#loader-service)
- [Contibution](#contributing-to-bqtail)
- [License](#license)


## Motivation

The goal of this library is to extend and simplify interaction with database/sql api.
This library defines
- api to access database dictionary metadata
- services for reading/inserting/loading/updating/deleting


## Usage

### Dictionary metadata

```go
package mypkg

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"log"
)

func ExampleService_Info() {
	dsn := ""
	driver := ""
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatalln(err)
	}

	meta := metadata.New()
	{
		tables := []sink.Table{}
		catalog := ""
		schema := "mydb"
		err := meta.Info(context.TODO(), db, info.KindTables, &tables, option.NewArgs(catalog, schema))
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(tables)
	}

	{
		columnes := []sink.Column{}
		catalog := ""
		schema := "mydb"
		table := "myTable"
		err := meta.Info(context.TODO(), db, info.KindTable, &columnes, option.NewArgs(catalog, schema, table))
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(columnes)
	}
}
```

The following info kind and sink are available:

- info.KindVersion: database version
- info.KindCatalogs: catalogs
- info.KindCatalog: catalog for provided catalog name
- info.KindCurrentSchema: current schema
- info.KindSchemas ([]sink.Schema): list of schema for provided catalog
- info.KindSchema ([]sink.Schema): list of schema for provided catalog, schema name
- info.KindTables ([]sink.Table): list of tables for provided catalog, schema
- info.KindTable ([]sink.Column): table columns info for provided catalog, schema, table name
- info.KindViews ([]sink.Table): list of views for provided catalog, schema
- info.KindView ([]sink.Column): view columns info for provided catalog, schema, view name
- info.KindPrimaryKeys ([]sink.Key) list of primary keys for provided catalog, schema, table name
- info.KindForeignKeys ([]sink.Key) list of foreign keys for provided catalog, schema, table name
- info.KindConstraints ([]sink.Key) list of constraints keys for provided catalog, schema, table name
- info.KindIndexes: ([]sink.Index) list of indexes for provided catalog, schema, table name
- info.KindIndex: ([]sink.Index) list of indexes for provided catalog, schema, table name, index name
- info.KindSequences:([]sink.Sequence) list of sequences values for catalog, schema
- info.KindFunctions: ([]sink.Function) list of functions for catalog, schema
- info.KindSession:  ([]sink.Session) list of session

### I/O Services

### Reader Service

```go
package mypkg

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/read"
	"log"
)

func ExampleReader_ReadAll() {
	dsn := ""
	driver := ""
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatalln(err)
	}
	ctx := context.Background()
	type Foo struct {
		ID     int
		Name   string
		Active bool
	}
	newFoo := func() interface{} { return &Foo{} }
	reader, err := read.New(ctx, db, "SELECT * FROM foo", newFoo)
	if err != nil {
		log.Fatalln(err)
	}
	var foos []*Foo
	err =  reader.QueryAll(ctx, func(row interface{}) error {
		foo := row.(*Foo)
		foos = append(foos, foo)
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("read foos: %+v\n", foos)
}
```

### Inserter Service

```go
package insert_test

import (
  "context"
  "database/sql"
  "fmt"
  "github.com/viant/sqlx/io/insert"
  //Make sure to add specific databas product import
  _ "github.com/viant/sqlx/metadata/product/mysql"
  "github.com/viant/sqlx/option"
  "log"
)

func ExampleService_Exec() {
  type Foo struct {
    ID   int
    Name string
  }
  dsn := ""
  db, err := sql.Open("mysql", dsn)
  if err != nil {
    log.Fatalln(err)
  }

  insert, err := insert.New(context.TODO(), db, "mytable", option.BatchSize(1024))
  if err != nil {
    log.Fatalln(err)
  }
  var records []*Foo
  //records = getAppRecords()

  affected, lastID, err := insert.Exec(context.TODO(), records)
  if err != nil {
    log.Fatalln(err)
  }
  fmt.Printf("affected: %v, last ID: %v\n", affected, lastID)
}

```

### Validator Service

Validator service has ability to validate unique,foreign key and not null constraints, with the following tag:
- unique,table
- notNull
- refColumn,refTable

For example:
```go

type Record struct {
    Id     int              `sqlx:"name=ID,autoincrement,primaryKey"`
    Name   *string          `sqlx:"name=name,unique,table=myTable" json:",omitempty"`
    DeptId *int             `sqlx:"name=name,refColumn=id,refTable=dep" json:",omitempty"`
    StartDate *time.Time    `sqlx:"name=startData,notNull" json:",omitempty"`
}
var db *sql.Db = nil //populate you db
var rec := &Record{}
validator := New()
err = validator.Validate(context.Background(), db, rec)

```



### Updater Service

### Merger Service

### Deleter Service

### Loader Service

```go
package load_test

import (
	"context"
	"database/sql"
	"fmt"
	
	//Make sure to import specific database loader implementation
     _ "github.com/viant/sqlx/metadata/product/mysql/load"
	"github.com/viant/sqlx/io/load"
	"log"
)

func ExampleService_Exec() {
	type Foo struct {
		ID int
		Name string
	}
	dsn := ""
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalln(err)
	}
	loader, err := load.New(context.Background(), db, "dest_table")
	if err != nil {
		log.Fatalln(err)
	}
	var data []Foo
	
	//data = getAppData()
	count,  err := loader.Exec(context.TODO(), &data)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("loaded %v\n", count)
}

```



### Supported tags (annotations)




## Contribution


sqlx is an open source project and contributors are welcome!

See [TODO](TODO.md) list


## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

## Authors

- Valery Carey
- Pawan Poudyal
- Kamil Larysz
- Adrian Witas
