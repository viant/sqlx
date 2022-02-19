# sqlx - Comprehensive SQL Extensions For Go

## Motivation

The goal of this library is to extend and simplify interaction with database/sql api.
This library defines
- api to access database dictionary metadata
- services for reading/inserting/loading/updating/deleting



### Accessing database dictionary metadata

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
- info.KindCurrentSchema: current schema
- info.KindSchemas ([]sink.Schema): list of schema for provided catalog
- info.KindTables ([]sink.Table): list of tables  for provided catalog, schema
- info.KindTable (sink.Table): table info for provided catalog, schema, table name
- info.KindViews ([]sink.Table): list of views  for provided catalog, schema
- info.KindViews(sink.Table): list of views  for provided catalog, schema, view name
- info.KindPrimaryKeys ([]sink.Key) ist of primary keys  for provided catalog, schema, table name
- info.KindForeignKeys ([]sink.Key) ist of primary keys  for provided catalog, schema, table name
- info.KindForeignKeys ([]sink.Key) ist of foreign keys  for provided catalog, schema, table name
- info.KindIndexes: ([]sink.Index) ist of indexes for provided catalog, schema, table name
- info.KindIndex: (sink.Index) list of indexes for provided catalog, schema, table name, index name
- info.KindSequences:(sink.Sequence) list of sequences values for catalog, schema
- info.KindFunctions: ([]sink.Function) list of functions for catalog, schema


### Reading data


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

### Inserting data




### Loading data




Before using any of the CRUD operation, You have to import package with required product. All of the supported products
are specified at the `sqlx/metadata/product` package e.g. If you want to use `MySQL` database you need to import `MySQL`
product:

```go
    import _ "github.com/viant/sqlx/metadata/product/mysql"
```

You also need to import `load` package if you want to use `sqlx/io/load` e.g. If you want to load data into `MySQL`
database, you need to import `MySQL` load package:

```go
    import _ "github.com/viant/sqlx/metadata/product/mysql/load"
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
