package cache

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"hash/fnv"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	NotExistStatus = iota
	InUseStatus
	ErrorStatus
	ExistsStatus
)

type (
	ScannerFn func(args ...interface{}) error
	Service   struct {
		storage   string
		afs       afs.Service
		ttl       time.Duration
		extension string

		scanTypes []*xunsafe.Type
		mux       sync.RWMutex
		cacheType reflect.Type
		xFields   []*xunsafe.Field
		signature string
		canWrite  map[string]bool
		stream    *option.Stream
		recorder  Recorder
	}
)

//NewCache creates new cache.
func NewCache(URL string, ttl time.Duration, signature string, stream *option.Stream, options ...interface{}) (*Service, error) {
	var recorder Recorder
	for _, anOption := range options {
		switch actual := anOption.(type) {
		case Recorder:
			recorder = actual
		}
	}

	if URL[len(URL)-1] != '/' {
		URL += "/"
	}
	cache := &Service{
		afs:       afs.New(),
		ttl:       ttl,
		storage:   URL,
		extension: ".json",
		signature: signature,
		canWrite:  map[string]bool{},
		stream:    stream,
		recorder:  recorder,
	}

	return cache, nil
}

func (c *Service) Get(ctx context.Context, SQL string, args []interface{}) (*Entry, error) {
	URL, err := c.generateURL(SQL, args)
	if err != nil {
		return nil, err
	}

	if c.mark(URL) {
		return nil, nil
	}

	entry, err := c.getEntry(ctx, SQL, args, err, URL)
	if err != nil || entry == nil {
		c.unmark(URL)
		return entry, err
	}

	if entry.Has() {
		c.unmark(URL)
	}

	return entry, err
}

func (c *Service) getEntry(ctx context.Context, SQL string, args []interface{}, err error, URL string) (*Entry, error) {
	argsMarshal, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	entry := &Entry{
		Meta: Meta{
			SQL:       SQL,
			Args:      argsMarshal,
			url:       URL,
			Signature: c.signature,
		},
	}

	status, err := c.updateEntry(ctx, err, URL, entry)
	if err != nil {
		return nil, err
	}

	switch status {
	case InUseStatus:
		return nil, nil
	case ErrorStatus:
		return nil, err
	}

	return entry, nil
}

func (c *Service) updateEntry(ctx context.Context, err error, URL string, entry *Entry) (int, error) {
	status, err := c.readData(ctx, entry)
	if status == NotExistStatus || status == InUseStatus || err != nil {
		if err == nil {
			c.mux.RLock()
			c.canWrite[URL] = false
			c.mux.RUnlock()
		}

		return status, err
	}

	metaCorrect, err := c.checkMeta(entry.reader, &entry.Meta)
	if !metaCorrect || err != nil {
		return status, c.afs.Delete(ctx, URL)
	}
	return status, nil
}

func (c *Service) checkMeta(dataReader *bufio.Reader, entryMeta *Meta) (bool, error) {
	data, err := readLine(dataReader)
	meta := Meta{}
	if err = json.Unmarshal(data, &meta); err != nil {
		return false, nil
	}

	if c.expired(meta) || c.wrongSignature(meta, entryMeta) || c.wrongSQL(meta, entryMeta) || c.wrongArgs(meta, entryMeta) {
		return false, nil
	}

	entryMeta.Type = meta.Type
	entryMeta.Fields = meta.Fields

	for _, field := range entryMeta.Fields {
		if err = field.init(); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (c *Service) generateURL(SQL string, args []interface{}) (string, error) {
	argMarshal, err := json.Marshal(args)
	if err != nil {
		return "", err
	}

	hasher := fnv.New64()
	_, err = hasher.Write(append([]byte(SQL), argMarshal...))

	if err != nil {
		return "", err
	}

	entryKey := strconv.Itoa(int(hasher.Sum64()))
	return c.storage + entryKey + c.extension, nil
}

func (c *Service) readData(ctx context.Context, entry *Entry) (int, error) {
	if ok, _ := c.afs.Exists(ctx, entry.Meta.url); !ok {
		return NotExistStatus, nil
	}

	afsReader, err := c.afs.OpenURL(ctx, entry.Meta.url, c.stream)
	if isRateError(err) || isPreConditionError(err) {
		return InUseStatus, nil
	}

	if err != nil {
		return ErrorStatus, err
	}

	reader := bufio.NewReader(afsReader)
	if err != nil {
		return ErrorStatus, err
	}

	entry.reader = reader
	entry.readCloser = afsReader
	return ExistsStatus, nil
}

func (c *Service) wrongArgs(meta Meta, entryMeta *Meta) bool {
	return !bytes.Equal(meta.Args, entryMeta.Args)
}

func (c *Service) wrongSQL(meta Meta, entryMeta *Meta) bool {
	return meta.SQL != entryMeta.SQL
}

func (c *Service) wrongSignature(meta Meta, entryMeta *Meta) bool {
	return meta.Signature != entryMeta.Signature
}

func (c *Service) expired(meta Meta) bool {
	return int(Now().UnixNano()) > meta.TimeToLive
}

func (c *Service) writeMeta(ctx context.Context, m *Entry) (*bufio.Writer, error) {
	writer, err := c.afs.NewWriter(ctx, m.Meta.url, 0644, &option.SkipChecksum{Skip: true})
	if err != nil {
		return nil, err
	}

	m.writeCloser = writer
	bufioWriter := bufio.NewWriterSize(writer, 2048)

	m.Meta.TimeToLive = int(Now().Add(c.ttl).UnixNano())
	data, err := json.Marshal(m.Meta)
	if err != nil {
		return nil, err
	}

	if err = c.write(bufioWriter, data, false); err != nil {
		return bufioWriter, err
	}

	return bufioWriter, nil
}

func (c *Service) write(bufioWriter *bufio.Writer, data []byte, addNewLine bool) error {
	if addNewLine {
		if err := bufioWriter.WriteByte('\n'); err != nil {
			return err
		}
	}

	_, err := bufioWriter.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (c *Service) init() error {
	numField := c.cacheType.NumField()
	c.xFields = make([]*xunsafe.Field, numField)
	c.scanTypes = make([]*xunsafe.Type, numField)

	for i := 0; i < numField; i++ {
		c.xFields[i] = xunsafe.FieldByIndex(c.cacheType, i)
		c.scanTypes[i] = xunsafe.NewType(c.cacheType.Field(i).Type)
	}

	return nil
}

func (c *Service) UpdateType(ctx context.Context, entry *Entry, values []interface{}) (bool, error) {
	c.initializeCacheType(values)

	if entry.Meta.Type != "" && entry.Meta.Type != c.cacheType.String() {
		return false, c.Delete(ctx, entry)
	}

	entry.Meta.Type = c.cacheType.String()
	return true, nil
}

func (c *Service) initializeCacheType(values []interface{}) {
	if c.cacheType != nil {
		return
	}
	c.mux.Lock()
	defer c.mux.Unlock()

	fields := make([]reflect.StructField, len(values))
	c.scanTypes = make([]*xunsafe.Type, len(values))
	for i, value := range values {
		rValue := reflect.ValueOf(value)
		valueType := rValue.Type()
		fields[i] = reflect.StructField{Name: "Args" + strconv.Itoa(i), Type: valueType}
		c.scanTypes[i] = xunsafe.NewType(valueType.Elem())
	}

	c.cacheType = reflect.StructOf(fields)
	c.xFields = make([]*xunsafe.Field, len(values))
	for i := 0; i < c.cacheType.NumField(); i++ {
		c.xFields[i] = xunsafe.FieldByIndex(c.cacheType, i)
	}
}

func (c *Service) Delete(ctx context.Context, entry *Entry) error {
	return c.afs.Delete(ctx, entry.Meta.url)
}

func (c *Service) mark(URL string) bool {
	c.mux.RLock()
	_, isInMap := c.canWrite[URL]
	c.canWrite[URL] = false
	c.mux.RUnlock()
	return isInMap
}

func (c *Service) unmark(url string) {
	c.mux.RLock()
	delete(c.canWrite, url)
	c.mux.RUnlock()
}

func (c *Service) scanner(e *Entry) ScannerFn {
	return func(values ...interface{}) error {
		if c.recorder != nil {
			c.recorder.ScanValues(values)
		}

		cachedObj := reflect.New(c.cacheType)
		var err error
		if err = json.Unmarshal(e.Data, cachedObj.Interface()); err != nil {
			return err
		}

		asInterface := cachedObj.Interface()
		asPtr := xunsafe.AsPointer(asInterface)

		for i, xField := range c.xFields {
			value := xField.Value(asPtr)
			destPtr := xunsafe.AsPointer(values[i])
			srcPtr := xunsafe.AsPointer(value)
			if destPtr == nil || srcPtr == nil {
				continue
			}

			xunsafe.Copy(destPtr, srcPtr, int(c.scanTypes[i].Type().Size()))
		}

		e.index++
		return err
	}
}

func (c *Service) Close(ctx context.Context, e *Entry) error {
	err := c.close(e)
	if err != nil {
		_ = c.Delete(ctx, e)
		return err
	}

	return nil
}

func (c *Service) close(e *Entry) error {
	if e.Has() {
		return e.readCloser.Close()
	}

	c.unmark(e.Meta.url)
	if err := e.writer.Flush(); err != nil {
		return err
	}

	if err := e.writeCloser.Close(); err != nil {
		return err
	}

	return nil
}

func (c *Service) AddValues(ctx context.Context, e *Entry, values []interface{}) error {
	if c.recorder != nil {
		c.recorder.AddValues(values)
	}

	err := c.addRow(ctx, e, values)
	if err != nil && e.writeCloser != nil {
		_ = e.writeCloser.Close()
	}

	return err
}

func (c *Service) AssignRows(entry *Entry, rows *sql.Rows) error {
	if len(entry.Meta.Fields) > 0 {
		return nil
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	ioColumns := io.TypesToColumns(types)
	entry.Meta.Fields = make([]*Field, len(ioColumns))

	for i, column := range ioColumns {
		length, _ := column.Length()
		precision, scale, _ := column.DecimalSize()
		nullable, _ := column.Nullable()
		entry.Meta.Fields[i] = &Field{
			ColumnName:         column.Name(),
			ColumnLength:       length,
			ColumnPrecision:    precision,
			ColumnScale:        scale,
			ColumnScanType:     column.ScanType().String(),
			_columnScanType:    column.ScanType(),
			ColumnNullable:     nullable,
			ColumnDatabaseName: column.DatabaseTypeName(),
			ColumnTag:          column.Tag(),
		}
	}

	return nil
}
