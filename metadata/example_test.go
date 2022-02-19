package metadata_test

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
		tables := "myTable"
		err := meta.Info(context.TODO(), db, info.KindTable, &columnes, option.NewArgs(catalog, schema, tables))
		if err != nil {
			log.Fatalln(err)
		}
		log.Println(columnes)
	}
}
