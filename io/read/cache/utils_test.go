package cache

import (
	"bufio"
	"strings"
	"testing"
)

func TestReadLine_LongBufferedLine(t *testing.T) {
	first := strings.Repeat(`{"column":"value"}`, 400)
	reader := bufio.NewReaderSize(strings.NewReader(first+"\nsecond\n"), 32)

	line, err := ReadLine(reader)
	if err != nil {
		t.Fatalf("ReadLine(first) error = %v", err)
	}
	if string(line) != first {
		t.Fatalf("unexpected first line length=%d want=%d", len(line), len(first))
	}

	line, err = ReadLine(reader)
	if err != nil {
		t.Fatalf("ReadLine(second) error = %v", err)
	}
	if string(line) != "second" {
		t.Fatalf("unexpected second line %q", string(line))
	}
}
