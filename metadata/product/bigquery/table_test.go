package bigquery

import (
	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"testing"
)

func TestTableFieldTypeRange(t *testing.T) {
	for _, bound := range []string{"DATE", "DATETIME", "TIMESTAMP"} {
		t.Run(bound, func(t *testing.T) {
			field := &api.TableFieldSchema{Name: "window", Type: "RANGE", RangeElementType: &api.TableFieldSchemaRangeElementType{Type: bound}}
			require.Equal(t, "RANGE<"+bound+">", tableFieldType(field))
			field.Mode = "REPEATED"
			require.Equal(t, "ARRAY<RANGE<"+bound+">>", tableFieldType(field))
			record := &api.TableFieldSchema{Type: "RECORD", Fields: []*api.TableFieldSchema{field}}
			require.Equal(t, "STRUCT<`window` ARRAY<RANGE<"+bound+">>>", tableFieldType(record))
			record.Mode = "REPEATED"
			require.Equal(t, "ARRAY<STRUCT<`window` ARRAY<RANGE<"+bound+">>>>", tableFieldType(record))
		})
	}
}
