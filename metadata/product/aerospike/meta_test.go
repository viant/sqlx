package aerospike

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/aerospike"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"github.com/viant/toolbox"
	"log"
	"testing"
)

// TODO rebuild and add expected
// TODO just printing for now
func TestService_GetMetadataMySQL(t *testing.T) {

	db, err := sql.Open("aerospike", "aerospike://127.0.0.1:3000/test")
	assert.Nil(t, err, "GetMetadataMySQL - error while testing")

	defer db.Close()
	err = GetMetadataAerospike(db)
	assert.Nil(t, err, "GetMetadataMySQL - error while getting metadata")
}

func GetMetadataAerospike(db *sql.DB) (err error) {

	/*
		        00 meta.DetectProduct
				01 info.KindVersion: database version
				02 info.KindCatalogs: catalogs
			    03 info.KindCatalog: catalog
				04 info.KindCurrentSchema: current schema
				05 info.KindSchemas ([]sink.Schema): list of schema for provided catalog
			    06 info.KindSchema: schema

				07 info.KindTables ([]sink.Table): list of tables for provided catalog, schema
				08 info.KindTable ([]sink.Column): table columns info for provided catalog, schema, table name
				09 info.KindViews ([]sink.Table): list of views for provided catalog, schema
				10 info.KindView ([]sink.Column): view columns info for provided catalog, schema, view name
				11 info.KindPrimaryKeys ([]sink.Key) list of primary keys for provided catalog, schema, table name
				12 info.KindForeignKeys ([]sink.Key) list of foreign keys for provided catalog, schema, table name
				13 info.KindConstraints ([]sink.Key) list of constraints keys for provided catalog, schema, table name
				14 info.KindIndexes: ([]sink.Index) list of indexes for provided catalog, schema, table name
				15 info.KindIndex: (sink.Index) index for provided catalog, schema, table name, index name
				16 info.KindSequences:([]sink.Sequence) list of sequences values for catalog, schema
				17 info.KindFunctions: ([]sink.Function) list of functions for catalog, schema
				18 info.KindSession:  ([]sink.Session) list of session
				// NOT IMPLEMENTED FOR VERTICA info.KindForeignKeysCheckOn:
				// NOT IMPLEMENTED FOR VERTICA info.KindForeignKeysCheckOff:
	*/

	if err != nil {
		panic(err.Error())
	}

	fmt.Println("### ######################################################################################")
	fmt.Println("###  METADATA FOR AEROSPIKE ###")
	fmt.Println("### ######################################################################################")
	meta := metadata.New()

	catalogGlobal := ""
	if catalogGlobal == "" {
	}

	fmt.Println("\n### 00 meta.DetectProduct Product ###")
	{
		product, err := meta.DetectProduct(context.TODO(), db)
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(product)
		toolbox.DumpIndent(product, true)
	}

	fmt.Println("\n### 01 info.KindVersion: database version ###")
	{

		result := []string{}
		err = meta.Info(context.TODO(), db, info.KindVersion, &result)
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 01 info.KindSchemas ([]sink.Schema): list of all schemas ###")
	{
		result := []sink.Schema{}
		err = meta.Info(context.TODO(), db, info.KindSchemas, &result)
		if err != nil {
			log.Fatalln(err)
		}
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 02a info.KindSchema ([]sink.Schema): list all schemas ###")
	{
		result := []sink.Schema{}
		//catalog := ""
		//schema := "test"
		err = meta.Info(context.TODO(), db, info.KindSchema, &result /*, option.NewArgs(catalog, schema)*/)
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 02a info.KindSchema ([]sink.Schema): list no schemas (schema with name 'none') ###")
	{
		result := []sink.Schema{}
		catalog := ""
		schema := "none"
		err = meta.Info(context.TODO(), db, info.KindSchema, &result, option.NewArgs(catalog, schema))
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 02b info.KindSchema ([]sink.Schema): list one schema with name 'test' ###")
	{
		result := []sink.Schema{}
		//catalog := ""
		schema := "test"
		err = meta.Info(context.TODO(), db, info.KindSchema, &result, option.NewArgs(schema))
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 03 info.KindTables ([]sink.Table): list all tables ###")
	{
		result := []sink.Table{}
		//catalog := ""
		//schema := "public"
		err := meta.Info(context.TODO(), db, info.KindTables, &result) //, option.NewArgs(catalog, schema))
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}
	fmt.Println("\n### 03b info.KindTables ([]sink.Table): list one table with name 'tables' ###")
	{
		result := []sink.Table{}
		catalog := ""
		schema_but_table_passed := "tables" // TODO fake variable
		err := meta.Info(context.TODO(), db, info.KindTables, &result, option.NewArgs(catalog, schema_but_table_passed))
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(result)
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 04a info.KindTable (sink.Table): list all columns for table 'tables' ###")
	{
		result := []sink.Column{}
		catalog := ""
		schema := ""
		table := "tables"

		err := meta.Info(context.TODO(), db, info.KindTable, &result, option.NewArgs(catalog, schema, table))
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(columnes)
		toolbox.DumpIndent(result, true)

		//for _, column := range result {
		//	fmt.Printf("Column %s is unique: %v\n", column.Name, column.IsUnique())
		//}
	}

	fmt.Println("\n### 04b info.KindTable (sink.Table): list all table's columns ###")
	{
		result := []sink.Column{}
		/*		catalog := ""
				schema := "sakila"
				table := "actor"
		*/
		err := meta.Info(context.TODO(), db, info.KindTable, &result) //, option.NewArgs(catalog, schema, table))
		if err != nil {
			log.Fatalln(err)
		}
		toolbox.DumpIndent(result, true)
	}

	fmt.Println("\n### 18 info.KindSession:  ([]sink.Session) list of session ###")
	{
		result := []sink.Session{}
		err := meta.Info(context.TODO(), db, info.KindSession, &result)
		if err != nil {
			log.Fatalln(err)
		}
		//log.Println(columnes)
		toolbox.DumpIndent(result, true)
	}
	return err
}
