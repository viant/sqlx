package load_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/load"
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
	loader, err := load.New(context.Background(), db, "dest_table")
	if err != nil {
		log.Fatalln(err)
	}
	var data []Foo

	//data = getAppData()
	count, err := loader.Exec(context.TODO(), &data)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("loaded %v\n", count)
}
