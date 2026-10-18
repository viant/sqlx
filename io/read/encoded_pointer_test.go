package read

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io"
)

func TestEncodedPointerUsesModelDestination(t *testing.T) {
	type record struct {
		Raw   *json.RawMessage `sqlx:"raw,enc=JSON"`
		Bytes *[]byte          `sqlx:"bytes,enc=JSON"`
		Plain *[]byte          `sqlx:"plain"`
	}
	columns := []io.Column{io.NewColumn("raw", "BLOB", reflect.TypeOf([]byte{})), io.NewColumn("bytes", "BLOB", reflect.TypeOf([]byte{})), io.NewColumn("plain", "BLOB", reflect.TypeOf([]byte{}))}
	mapper, err := NewSQLStructMapper(columns, reflect.TypeOf(record{}), nil)
	require.NoError(t, err)
	for _, raw := range []string{`[{"from":null,"to":0}]`, `{"enabled":false}`, `null`} {
		t.Run(raw, func(t *testing.T) {
			actual := record{}
			values, err := mapper(&actual)
			require.NoError(t, err)
			require.NoError(t, values[0].(sql.Scanner).Scan([]byte(raw)))
			if raw == "null" {
				require.Nil(t, actual.Raw)
			} else {
				require.NotNil(t, actual.Raw)
				require.JSONEq(t, raw, string(*actual.Raw))
			}
			require.NoError(t, values[1].(sql.Scanner).Scan(`"AQID"`))
			require.Equal(t, []byte{1, 2, 3}, *actual.Bytes)
			source := []byte{4, 5, 6}
			require.NoError(t, values[2].(sql.Scanner).Scan(&source))
			require.Equal(t, source, *actual.Plain)
			source[0] = 9
			require.Equal(t, byte(4), (*actual.Plain)[0])
		})
	}
}
