package read_test

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/read"
	"log"
)

func ExampleReader_ReadAll() {
	dsn := ""
	db, err := sql.Open("mysql", dsn)
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
	reader.QueryAll(ctx, func(row interface{}) error {
		foo := row.(*Foo)
		foos = append(foos, foo)
		return nil
	}, nil)
	log.Printf("read foos: %+v\n", foos)
}
