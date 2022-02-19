package insert_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/insert"
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
