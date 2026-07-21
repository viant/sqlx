package aerospike

import (
	"encoding/json"
	"errors"
	as "github.com/aerospike/aerospike-client-go"
	sio "io"
	"strings"
	"testing"
)

func TestIndexedValueAppendRow_ReturnsNetBufferedDelta(t *testing.T) {
	indexed, err := newIndexedValue(nil, nil, "", "/v1/api/test", as.BinMap{}, true)
	if err != nil {
		t.Fatalf("newIndexedValue() error = %v", err)
	}

	firstDelta, err := indexed.appendRow([]interface{}{"alpha"})
	if err != nil {
		t.Fatalf("appendRow(first) error = %v", err)
	}
	if firstDelta != len(`["alpha"]`) {
		t.Fatalf("unexpected first delta %d", firstDelta)
	}

	secondDelta, err := indexed.appendRow([]interface{}{"beta"})
	if err != nil {
		t.Fatalf("appendRow(second) error = %v", err)
	}
	if secondDelta != len("\n")+len(`["beta"]`) {
		t.Fatalf("unexpected second delta %d", secondDelta)
	}

	if got, want := indexed.buffer.String(), "[\"alpha\"]\n[\"beta\"]"; got != want {
		t.Fatalf("buffer = %q, want %q", got, want)
	}
}

func TestIndexedValueChunkKey(t *testing.T) {
	indexed := &indexedValue{baseKey: "warmup-key"}

	if got, want := indexed.chunkKey(0), "warmup-key"; got != want {
		t.Fatalf("chunkKey(0) = %q, want %q", got, want)
	}
	if got, want := indexed.chunkKey(3), "warmup-key#3"; got != want {
		t.Fatalf("chunkKey(3) = %q, want %q", got, want)
	}
}

func TestCloneBinMap(t *testing.T) {
	source := as.BinMap{
		sqlBin:  "select 1",
		argsBin: "[]",
	}

	cloned := cloneBinMap(source)
	cloned[sqlBin] = "changed"

	if got, want := source[sqlBin], "select 1"; got != want {
		t.Fatalf("source mutated to %v, want %v", got, want)
	}
}

func TestIndexedReadBins_IncludesCompressedChildData(t *testing.T) {
	bins := indexedReadBins()
	if len(bins) != 3 {
		t.Fatalf("unexpected bins length %d", len(bins))
	}
	if bins[0] != dataBin || bins[1] != compDataBin || bins[2] != childBin {
		t.Fatalf("unexpected bins %v", bins)
	}
}

func TestIndexedValueCompressedChunks_ReadAcrossChildChain(t *testing.T) {
	records := map[string]*as.Record{}
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			records[key.String()] = &as.Record{Key: key, Bins: cloneBinMap(binMap)}
			return nil
		},
		getRecordFn: func(key *as.Key, bins ...string) (*as.Record, error) {
			record, ok := records[key.String()]
			if !ok {
				return nil, errors.New("not found")
			}
			filtered := as.BinMap{}
			for _, bin := range bins {
				if value, exists := record.Bins[bin]; exists {
					filtered[bin] = value
				}
			}
			return &as.Record{Key: key, Bins: filtered}, nil
		},
	}

	indexed, err := newIndexedValue(aCache, 42, "order_id", "warmup-base", as.BinMap{
		sqlBin:  "SELECT 1",
		argsBin: "[]",
	}, false)
	if err != nil {
		t.Fatalf("newIndexedValue() error = %v", err)
	}

	var wantLines []string
	largeValue := strings.Repeat("x", compressionThreshold*2)
	rowCount := 1000
	for i := 0; i < rowCount; i++ {
		row := []interface{}{i, largeValue}
		if _, err = indexed.appendRow(row); err != nil {
			t.Fatalf("appendRow(%d) error = %v", i, err)
		}
		encoded, marshalErr := json.Marshal(row)
		if marshalErr != nil {
			t.Fatalf("json.Marshal(row) error = %v", marshalErr)
		}
		wantLines = append(wantLines, string(encoded))
	}

	if err = indexed.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}

	if len(records) < 2 {
		t.Fatalf("expected multiple chunk records, got %d", len(records))
	}

	rootKey, err := aCache.key(indexed.chunkKey(0))
	if err != nil {
		t.Fatalf("key() error = %v", err)
	}
	rootRecord, err := aCache.getRecord(rootKey, indexedReadBins()...)
	if err != nil {
		t.Fatalf("getRecord(root) error = %v", err)
	}

	reader := &Reader{
		namespace: aCache.namespace,
		cache:     aCache,
		record:    rootRecord,
		key:       rootKey,
		set:       aCache.set,
	}

	var gotLines []string
	for {
		line, _, readErr := reader.ReadLine()
		if readErr != nil {
			if errors.Is(readErr, sio.EOF) {
				break
			}
			t.Fatalf("ReadLine() error = %v", readErr)
		}
		gotLines = append(gotLines, string(line))
	}

	if len(gotLines) != len(wantLines) {
		t.Fatalf("unexpected line count %d, want %d", len(gotLines), len(wantLines))
	}
	for i := range wantLines {
		if gotLines[i] != wantLines[i] {
			t.Fatalf("line %d = %q, want %q", i, gotLines[i], wantLines[i])
		}
	}
}

func TestIndexedValueIndexedRootDoesNotCopyMetaBins(t *testing.T) {
	records := map[string]as.BinMap{}
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			records[key.String()] = cloneBinMap(binMap)
			return nil
		},
	}

	indexed, err := newIndexedValue(aCache, 42, "order_id", "warmup-base", as.BinMap{
		sqlBin:    "SELECT 1",
		argsBin:   "[]",
		fieldsBin: "[{\"name\":\"order_id\"}]",
		columnBin: "order_id",
	}, false)
	if err != nil {
		t.Fatalf("newIndexedValue() error = %v", err)
	}

	if _, err = indexed.appendRow([]interface{}{"alpha"}); err != nil {
		t.Fatalf("appendRow() error = %v", err)
	}
	if err = indexed.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	for _, record := range records {
		if _, ok := record[sqlBin]; ok {
			t.Fatalf("unexpected %s bin on indexed child root", sqlBin)
		}
		if _, ok := record[argsBin]; ok {
			t.Fatalf("unexpected %s bin on indexed child root", argsBin)
		}
		if _, ok := record[fieldsBin]; ok {
			t.Fatalf("unexpected %s bin on indexed child root", fieldsBin)
		}
		if _, ok := record[columnBin]; ok {
			t.Fatalf("unexpected %s bin on indexed child root", columnBin)
		}
		if _, ok := record[dataBin]; !ok {
			t.Fatalf("expected %s bin on indexed child root", dataBin)
		}
	}
}

func TestIndexedValueSingleRootKeepsMetaBins(t *testing.T) {
	records := map[string]as.BinMap{}
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			records[key.String()] = cloneBinMap(binMap)
			return nil
		},
	}

	indexed, err := newIndexedValue(aCache, nil, "", "warmup-base", as.BinMap{
		sqlBin:    "SELECT 1",
		argsBin:   "[]",
		fieldsBin: "[{\"name\":\"order_id\"}]",
		columnBin: "",
	}, true)
	if err != nil {
		t.Fatalf("newIndexedValue() error = %v", err)
	}

	if _, err = indexed.appendRow([]interface{}{"alpha"}); err != nil {
		t.Fatalf("appendRow() error = %v", err)
	}
	if err = indexed.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	for _, record := range records {
		if _, ok := record[sqlBin]; !ok {
			t.Fatalf("expected %s bin on single root", sqlBin)
		}
		if _, ok := record[argsBin]; !ok {
			t.Fatalf("expected %s bin on single root", argsBin)
		}
		if _, ok := record[fieldsBin]; !ok {
			t.Fatalf("expected %s bin on single root", fieldsBin)
		}
		if _, ok := record[dataBin]; !ok && record[compDataBin] == nil {
			t.Fatalf("expected payload bin on single root")
		}
	}
}

func TestSingleSourceClose_WritesEmptyRootWhenNoRowsSeen(t *testing.T) {
	records := map[string]as.BinMap{}
	aCache := &Cache{
		namespace: "ns_memory",
		set:       "steward_test",
		putFn: func(key *as.Key, binMap as.BinMap) error {
			records[key.String()] = cloneBinMap(binMap)
			return nil
		},
	}

	source := NewSingleSource(aCache, "", "warmup-base", as.BinMap{
		sqlBin:    "SELECT 1",
		argsBin:   "[]",
		fieldsBin: "[{\"name\":\"order_id\"}]",
	})

	if err := source.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if source.Count() != 1 {
		t.Fatalf("unexpected count %d, want 1", source.Count())
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	for _, record := range records {
		if _, ok := record[sqlBin]; !ok {
			t.Fatalf("expected %s bin on empty single root", sqlBin)
		}
		if _, ok := record[argsBin]; !ok {
			t.Fatalf("expected %s bin on empty single root", argsBin)
		}
		if _, ok := record[fieldsBin]; !ok {
			t.Fatalf("expected %s bin on empty single root", fieldsBin)
		}
		if got, ok := record[dataBin]; !ok || got.(string) != "" {
			t.Fatalf("expected empty %s bin, got %v", dataBin, got)
		}
	}
}
