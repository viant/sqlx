package csv

import (
	"io"
	"testing"
	"time"

	sqlio "github.com/viant/sqlx/io"
)

func TestReaderTimeLayoutOption(t *testing.T) {
	input := []struct{ Time time.Time }{{time.Date(2026, 11, 9, 12, 34, 56, 0, time.UTC)}}
	for _, test := range []struct {
		name    string
		options []interface{}
		want    string
	}{
		{name: "shared default unchanged", want: "2026-11-09T12:34:56Z"},
		{name: "numeric offset option", options: []interface{}{sqlio.StringifierTimeLayout("2006-01-02T15:04:05-07:00")}, want: "2026-11-09T12:34:56+00:00"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader, _, err := NewReader(input, &Config{}, test.options...)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if string(actual) != test.want {
				t.Fatalf("got %q, want %q", actual, test.want)
			}
		})
	}
}
