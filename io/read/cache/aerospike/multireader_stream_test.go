package aerospike

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/sqlx/io/read/cache"
)

type failedStream struct{ err error }

func (r failedStream) Read([]byte) (int, error) { return 0, r.err }

func TestMultiReaderStreamUsesLogicalRowWindows(t *testing.T) {
	first := fmt.Sprintf("[1,%q]\n", strings.Repeat("a", 9000))
	second := fmt.Sprintf("[2,%q]\n", strings.Repeat("b", 9000))
	for _, lineMode := range []bool{false, true} {
		t.Run(fmt.Sprint(lineMode), func(t *testing.T) {
			m := NewMultiReader(&cache.ParmetrizedQuery{Offset: 1, Limit: 1})
			m.AddReader(&Reader{reader: bufio.NewReaderSize(strings.NewReader(first+second+"[3,\"skip\"]\n"), 16), record: &as.Record{Bins: as.BinMap{}}})
			defer m.Close()
			var actual bytes.Buffer
			if lineMode {
				for {
					line, prefix, err := m.ReadLine()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					actual.Write(line)
					if !prefix {
						actual.WriteByte('\n')
					}
				}
			} else {
				target := make([]byte, 7)
				for {
					n, err := m.Read(target)
					actual.Write(target[:n])
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if n == 0 {
						t.Fatal("reader made no progress")
					}
				}
			}
			if actual.String() != second {
				t.Fatalf("logical window returned %d bytes, want %d", actual.Len(), len(second))
			}
		})
	}
}

func TestMultiReaderPropagatesStreamErrors(t *testing.T) {
	sentinel := errors.New("source failure")
	for _, offset := range []int{0, 1} {
		m := NewMultiReader(&cache.ParmetrizedQuery{Offset: offset})
		m.AddReader(&Reader{reader: bufio.NewReader(failedStream{sentinel}), record: &as.Record{Bins: as.BinMap{}}})
		_, err := io.ReadAll(m)
		if !errors.Is(err, sentinel) {
			t.Fatalf("offset %d: error=%v", offset, err)
		}
		_ = m.Close()
	}
}
