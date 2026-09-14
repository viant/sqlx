package io

import (
	"reflect"
	"strconv"
	"testing"
)

func TestParseTagRetainsSQLIdentifiersOnly(t *testing.T) {
	for _, table := range []string{"`project.dataset.table`", "[project.dataset.table]", "[project:dataset.table]", `"odd.table"`, "[odd name]"} {
		text := "id,table=" + table + ",refTable=" + table + ",db=\"project\",refDb=`project.dataset`,refColumn=\"id\",unique,required=false,errormsg='with,comma',generator=\"default\""
		tag := ParseTag(reflect.StructTag("sqlx:" + strconv.Quote(text)))
		if tag.Table != table || tag.RefTable != table || tag.Db != `"project"` || tag.RefDb != "`project.dataset`" {
			t.Fatalf("lost SQL spelling: %+v", tag)
		}
		if tag.RefColumn != "id" || tag.ErrorMgs != "'with,comma'" || tag.Generator != "default" || !tag.IsUnique || tag.Required {
			t.Fatalf("changed unrelated values: %+v", tag)
		}
	}
	text := "id,refTable=`real.table`,errormsg='message,refTable=wrong',type=`string`"
	tag := ParseTag(reflect.StructTag("sqlx:" + strconv.Quote(text)))
	if tag.RefTable != "`real.table`" || tag.ErrorMgs != "'message,refTable=wrong'" || tag.DataType != "string" {
		t.Fatalf("quoted message reinterpreted: %+v", tag)
	}
}
