package aerospike

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/google/uuid"
	"github.com/viant/parsly/matcher"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/hash"
	sio "io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	sqlBin      = "SQL"
	argsBin     = "Args"
	dataBin     = "Data"
	compDataBin = "CData"
	typesBin    = "Type"
	fieldsBin   = "Fields"
	childBin    = "Child"
	columnBin   = "Column"
)

var cachedBins = []string{typesBin, argsBin, sqlBin, dataBin, fieldsBin, compDataBin}

type (
	Cache struct {
		recorder        cache.Recorder
		typeHolder      *cache.ScanTypeHolder
		client          *as.Client
		set             string
		namespace       string
		mux             sync.Mutex
		timeToLiveInSec uint32
		allowSmart      bool
		chanSize        int
		timeoutConfig   *TimeoutConfig
		failureHandler  *FailureHandler
	}
)

func (a *Cache) IndexBy(ctx context.Context, db *sql.DB, column, SQL string, args []interface{}, options ...interface{}) (int, error) {
	if args == nil {
		args = []interface{}{}
	}

	querySQL, isOrdered := tryOrderedSQL(SQL, column)
	rows, err := db.Query(querySQL, args...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = rows.Close()
	}()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}

	fields, err := cache.ColumnsToFields(io.TypesToColumns(columnTypes))
	if err != nil {
		return 0, err
	}

	var values = make(chan *cache.Indexed, 512)
	errors := &Errors{}
	go func() {
		err = a.fetchAndIndexValues(ctx, fields, column, rows, values, isOrdered)
		errors.Add(err)
		close(values)
	}()

	identitySQL, identityArgs, argsMarshal, meta, err := a.resolveIndexIdentity(SQL, args, options...)
	if err != nil {
		return 0, err
	}
	identitySQL, argsMarshal, canonicalization := canonicalWarmupIdentity(identitySQL, argsMarshal)
	URL, err := a.identityURL(identitySQL, identityArgs, argsMarshal)
	if err != nil {
		return 0, err
	}
	a.logWarmupIdentityResolved("index_write", column, URL, identitySQL, argsMarshal, canonicalization, meta)
	if column != "" {
		a.logWarmupMarker("IndexBy", column, URL, a.columnURL(URL, column))
	}

	fieldMarshal, err := json.Marshal(fields)
	if err != nil {
		return 0, err
	}

	argsStringified := string(argsMarshal)
	fieldsStringified := string(fieldMarshal)

	inserted := 0
	for value := range values {
		var metaBin as.BinMap
		if column == "" {
			metaBin = a.metaBin(identitySQL, argsStringified, fieldsStringified, column)
		} else {
			metaBin = as.BinMap{}
		}

		errors.Add(a.writeIndexData(value, URL, column, metaBin))
		inserted++
	}

	if err = errors.Err(); err != nil {
		return inserted, err
	}

	if column != "" {
		return inserted + 1, a.putRowMarker(URL, column, a.metaBin(identitySQL, argsStringified, fieldsStringified, column))
	}

	return inserted, nil
}

func tryOrderedSQL(SQL string, column string) (string, bool) {
	if column == "" {
		return SQL, false
	}

	lcSQL := strings.ToLower(SQL)
	orderByIndex := strings.LastIndex(lcSQL, "order ")
	if orderByIndex != -1 && !matcher.IsWhiteSpace(lcSQL[orderByIndex-1]) {
		orderByIndex = -1
	}
	hasOrderBy := orderByIndex != -1
	if hasOrderBy {
		orderClause := string(lcSQL[orderByIndex+len("order ")])
		return SQL, strings.Contains(orderClause, strings.ToLower(column))
	}
	return SQL + " ORDER BY " + column, true
}

func (a *Cache) metaBin(SQL string, argsStringified string, fieldsStringified string, column string) as.BinMap {
	metaBin := as.BinMap{
		sqlBin:    SQL,
		argsBin:   argsStringified,
		fieldsBin: fieldsStringified,
		columnBin: column,
	}

	return metaBin
}

func (a *Cache) Rollback(ctx context.Context, entry *cache.Entry) error {
	return a.Delete(ctx, entry)
}

func (a *Cache) AsSource(ctx context.Context, entry *cache.Entry) (cache.Source, error) {
	return &Source{
		cache: a,
		entry: entry,
	}, nil
}

func (a *Cache) AddValues(ctx context.Context, entry *cache.Entry, values []interface{}) error {
	if a.recorder != nil {
		a.recorder.AddValues(values)
	}

	marshal, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return entry.Write(marshal)
}

func (a *Cache) Get(ctx context.Context, SQL string, args []interface{}, options ...interface{}) (*cache.Entry, error) {
	var query *cache.ParmetrizedQuery
	var cacheStats *cache.Stats
	var refresh bool
	for _, option := range options {
		switch actual := option.(type) {
		case *cache.ParmetrizedQuery:
			query = actual
		case *cache.Stats:
			cacheStats = actual
		case cache.Refresh:
			refresh = bool(actual)
		}
	}

	if cacheStats == nil {
		cacheStats = &cache.Stats{}
	}
	cacheStats.Init()
	if query != nil {
		query.Init()
	}

	if a.failureHandler != nil && a.failureHandler.IsProbing() {
		cacheStats.ErrorType = cache.ErrorTypeCurrentlyNotAvailable
		return nil, nil
	}
	return a.get(ctx, SQL, args, query, cacheStats, refresh)
}

func (a *Cache) get(ctx context.Context, SQL string, args []interface{}, columnsInMatcher *cache.ParmetrizedQuery, cacheStats *cache.Stats, refresh bool) (*cache.Entry, error) {
	lazyMatch, warmupMatch, err := a.readRecords(SQL, args, columnsInMatcher)
	if refresh {
		lazyMatch.hasKey = false
		lazyMatch.record = nil
	}
	a.updateCacheStats(lazyMatch, warmupMatch, cacheStats)
	cacheStats.ErrorType, cacheStats.ErrorCode, err = a.findActualError(err)
	if cacheStats.ErrorCode != types.OK && !cacheStats.FoundAny() || err != nil {
		a.handleResponseFailure(cacheStats.ErrorCode)
		return nil, err
	}

	jsonEncodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}

	expiryDuration := time.Second * time.Duration(a.timeToLiveInSec)
	anEntry := &cache.Entry{
		Meta: cache.Meta{
			SQL:          SQL,
			Args:         jsonEncodedArgs,
			ExpiryTimeMs: int(time.Now().Add(expiryDuration).UnixMilli()),
		},
		Id: a.entryId(lazyMatch, warmupMatch),
	}

	if err = a.updateLazyMatchEntry(ctx, anEntry, lazyMatch, SQL, jsonEncodedArgs, cacheStats); err != nil {
		return nil, err
	}

	if err = a.updateColumnsInMatchEntry(anEntry, warmupMatch, columnsInMatcher, cacheStats); err != nil {
		return nil, err
	}

	if err = a.updateMetaFields(anEntry, lazyMatch, warmupMatch); err != nil {
		return nil, err
	}

	return anEntry, a.updateWriter(anEntry, lazyMatch, SQL, jsonEncodedArgs, cacheStats)
}

func (a *Cache) updateCacheStats(fullMatch *RecordMatched, columnsInMatch *RecordMatched, cacheStats *cache.Stats) {
	if fullMatch.hasKey {
		cacheStats.Key = fullMatch.keyValue
	}
	if fullMatch.key != nil {
		cacheStats.Dataset = fullMatch.key.SetName()
		cacheStats.Namespace = fullMatch.key.Namespace()
	}
	cacheStats.FoundLazy = fullMatch != nil && fullMatch.hasKey
	cacheStats.FoundWarmup = columnsInMatch != nil && columnsInMatch.hasKey
}

// TODO mabe move to its own error type
func (a *Cache) findActualError(err error) (string, types.ResultCode, error) {
	if err == nil {
		return "", types.OK, nil
	}
	aerospikeErr, ok := asAerospikeErr(err)
	if !ok {
		if errors.Is(err, sio.EOF) {
			return "", types.TIMEOUT, err
		}

		return "", types.OK, nil
	}
	switch actual := aerospikeErr.ResultCode(); actual {
	case types.OK, types.KEY_NOT_FOUND_ERROR:
	//Do nothing
	case types.TIMEOUT, types.MAX_RETRIES_EXCEEDED:
		return cache.ErrorTypeTimeout, actual, nil
	case types.SERVER_NOT_AVAILABLE, types.NO_AVAILABLE_CONNECTIONS_TO_NODE, types.INVALID_NODE_ERROR:
		return cache.ErrorTypeServerUnavailable, actual, nil
	default:
		return cache.ErrorTypeServerGeneric, actual, err
	}
	return "", types.OK, nil
}

func (a *Cache) readRecords(SQL string, args []interface{}, query *cache.ParmetrizedQuery) (lazyMatch *RecordMatched, warmupMatch *RecordMatched, err error) {
	var errors = make([]error, 2)
	wg := sync.WaitGroup{}

	wg.Add(2)
	go func(SQL string, args []interface{}, wg *sync.WaitGroup) {
		defer wg.Done()
		lazyMatch, errors[0] = a.readRecord(SQL, args, nil)
	}(SQL, args, &wg)

	go func(query *cache.ParmetrizedQuery) {
		defer wg.Done()
		if query == nil {
			return
		}
		identitySQL, identityArgs, identityArgsMarshal, meta, e := query.WarmupIdentityResolved()
		if e != nil {
			errors[1] = e
			return
		}
		identitySQL, identityArgsMarshal, canonicalization := canonicalWarmupIdentity(identitySQL, identityArgsMarshal)
		if warmupURL, urlErr := a.identityURL(identitySQL, identityArgs, identityArgsMarshal); urlErr == nil {
			a.logWarmupIdentityResolved("read_lookup", query.By, warmupURL, identitySQL, identityArgsMarshal, canonicalization, meta)
		}
		warmupMatch, errors[1] = a.readRecord(identitySQL, identityArgs, identityArgsMarshal, func(aKey string) (string, error) {
			return a.columnURL(aKey, query.By), nil
		})
	}(query)
	wg.Wait()
	for i := range errors {
		if errors[i] == nil || a.isKeyNotFoundErr(errors[i]) {
			continue
		}
		err = errors[i]
		break
	}
	return lazyMatch, warmupMatch, err
}

func (a *Cache) readRecord(SQL string, args []interface{}, argsMarshal []byte, keyModifiers ...func(aKey string) (string, error)) (*RecordMatched, error) {
	var keyValue string
	var warmupURL string
	var err error

	keyValue, err = a.identityURL(SQL, args, argsMarshal)

	if err != nil {
		return nil, err
	}
	warmupURL = keyValue

	for _, modifier := range keyModifiers {
		keyValue, err = modifier(keyValue)
		if err != nil {
			return nil, err
		}
	}

	storeKey, err := a.key(keyValue)
	if err != nil {
		return nil, err
	}

	record, err := a.getRecord(storeKey, cachedBins...)
	if len(keyModifiers) > 0 {
		a.logWarmupRead(warmupURL, keyValue, record != nil && err == nil, err)
	}

	return &RecordMatched{
		key:      storeKey,
		record:   record,
		keyValue: keyValue,
		hasKey:   err == nil,
		baseURL:  warmupURL,
		args:     argsMarshal,
	}, err
}

func (a *Cache) readPolicy() *as.BasePolicy {
	policy := a.newBasePolicy(true)
	return policy
}

func (a *Cache) newBasePolicy(idempotent bool) *as.BasePolicy {
	policy := as.NewPolicy()
	if a.timeoutConfig != nil {
		if a.timeoutConfig.MaxRetries != 0 && idempotent {
			policy.MaxRetries = a.timeoutConfig.MaxRetries
		}
		if a.timeoutConfig.MaxRetries != 0 && idempotent {
			policy.SleepBetweenRetries = time.Millisecond * time.Duration(a.timeoutConfig.SleepBetweenRetriesMs)
		}
		if a.timeoutConfig.TotalTimeoutMs != 0 {
			policy.TotalTimeout = time.Millisecond * time.Duration(a.timeoutConfig.TotalTimeoutMs)
		}
	}
	return policy
}

func (a *Cache) AssignRows(entry *cache.Entry, rows *sql.Rows) error {
	return entry.AssignRows(rows)
}

func (a *Cache) UpdateType(ctx context.Context, entry *cache.Entry, args []interface{}) (bool, error) {
	a.ensureTypeHolder(args)

	if !a.typeHolder.Match(entry) {
		return false, a.Delete(ctx, entry)
	}

	return true, nil
}

func (a *Cache) Close(ctx context.Context, entry *cache.Entry) error {
	err := entry.Close()
	if err != nil {
		_ = a.Delete(ctx, entry)
		return err
	}

	return nil
}

func (a *Cache) Delete(ctx context.Context, entry *cache.Entry) error {
	key, err := a.key(entry.Id)
	if err != nil {
		return err
	}

	return a.deleteCascade(key)
}

func (a *Cache) deleteCascade(key *as.Key) error {
	var err error
	aRecord, _ := a.getRecord(key, childBin)
	var ok bool
	for aRecord != nil {
		if ok, err = a.client.Delete(a.writePolicy(), key); err != nil || !ok {
			return err
		}

		childKey := aRecord.Bins[childBin]
		if childKey == nil {
			return nil
		}

		key, err = a.key(childKey)
	}

	return nil
}

func (a *Cache) recordMatches(record *as.Record, SQL string, args []byte) bool {
	if record == nil {
		return false
	}

	sqlValue, ok := record.Bins[sqlBin].(string)
	if !ok || sqlValue != SQL {
		return false
	}

	argsValue, ok := record.Bins[argsBin].(string)
	if !ok || argsValue != string(args) {
		return false
	}

	return true
}

func (a *Cache) newWriter(key *as.Key, aKey string, SQL string, args []byte) *Writer {
	return &Writer{
		expirationTimeInSeconds: a.timeToLiveInSec,
		mainKey:                 key,
		buffers:                 []*bytes.Buffer{bytes.NewBuffer(nil)},
		id:                      aKey,
		sql:                     SQL,
		args:                    string(args),
		cache:                   a,
	}
}

func (a *Cache) key(keyValue interface{}) (*as.Key, error) {
	aKey, err := as.NewKey(a.namespace, a.set, keyValue)
	return aKey, err
}

func (a *Cache) reader(key *as.Key, record *as.Record) (*Reader, error) {

	return &Reader{
		key:       key,
		cache:     a,
		namespace: a.namespace,
		record:    record,
		set:       a.set,
	}, nil
}

func (a *Cache) ensureTypeHolder(values []interface{}) {
	if a.typeHolder != nil {
		return
	}

	a.mux.Lock()
	a.typeHolder = &cache.ScanTypeHolder{}
	a.typeHolder.InitType(values)
	a.mux.Unlock()
}

func (a *Cache) updateEntry(record *as.Record, entry *cache.Entry) error {
	return a.updateEntryFields(record, entry)
}

func (a *Cache) updateEntryFields(record *as.Record, entry *cache.Entry) error {
	fieldsValue := record.Bins[fieldsBin]
	if fieldsValue == nil {
		return nil
	}

	fieldsJSON, ok := fieldsValue.(string)
	if !ok {
		return fmt.Errorf("unexpected cache value type, expected %T got %T", fieldsJSON, fieldsValue)
	}

	var fields []*cache.Field
	if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
		return err
	}

	entry.Meta.Fields = fields

	for _, field := range entry.Meta.Fields {
		if err := field.Init(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Cache) writeIndexData(args *cache.Indexed, URL string, column string, metaBin as.BinMap) error {
	if args.ColumnValue == nil && args.Column != "" {
		return nil
	}

	marshal, err := json.Marshal(args.ColumnValue)
	if err != nil {
		return err
	}

	actualKey := a.columnValueURL(column, marshal, URL)
	key, err := a.key(actualKey)
	if err != nil {
		return err
	}

	data := args.Data.Bytes()
	isCompressed := false
	if len(data) > compressionThreshold {
		compressed, ok := compress(data)
		isCompressed = ok

		if ok {
			metaBin[compDataBin] = compressed
		}
	}

	if !isCompressed {
		metaBin[dataBin] = string(data)
	}

	return a.put(key, metaBin)
}

func compress(data []byte) ([]byte, bool) {
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	if _, err := sio.Copy(gzipWriter, bytes.NewBuffer(data)); err == nil {
		if err = gzipWriter.Flush(); err == nil {
			_ = gzipWriter.Close()
			return buffer.Bytes(), true
		}
	}
	return nil, false
}

func uncompress(data []byte) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	defer gzipReader.Close()
	buffer := bytes.NewBuffer(nil)
	_, err = sio.Copy(buffer, gzipReader)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (a *Cache) columnValueURL(column string, columnValueMarshal []byte, URL string) string {
	if column == "" {
		return URL
	}

	return strings.ToLower(column) + "#" + strconv.Quote(string(columnValueMarshal)) + "#" + URL
}

func (a *Cache) writePolicy() *as.WritePolicy {
	policy := as.NewWritePolicy(0, a.timeToLiveInSec)
	basePolicy := a.newBasePolicy(false)
	policy.BasePolicy = *basePolicy
	policy.SendKey = true

	return policy
}

func (a *Cache) putRowMarker(URL string, column string, bin as.BinMap) error {
	aKey, err := a.key(a.columnURL(URL, column))
	if err != nil {
		return err
	}

	return a.put(aKey, bin)
}

func (a *Cache) columnURL(URL string, column string) string {
	return strings.ToLower(column) + "#" + URL
}

func (a *Cache) identityURL(SQL string, args []interface{}, argsMarshal []byte) (string, error) {
	if argsMarshal == nil {
		return hash.GenerateURL(SQL, "", "", args)
	}
	return hash.GenerateWithMarshal(SQL, "", "", argsMarshal)
}

func canonicalWarmupIdentity(SQL string, argsMarshal []byte) (string, []byte, string) {
	if canonicalSQL, ok, detail := canonicalWarmupSQL(SQL); ok {
		return canonicalSQL, argsMarshal, detail
	}
	return SQL, argsMarshal, "fallback_parse_error"
}

func canonicalWarmupSQL(SQL string) (string, bool, string) {
	parsed, err := sqlparser.ParseQuery(SQL)
	if err != nil || parsed == nil {
		return "", false, "fallback_parse_error"
	}
	return normalizeWarmupSQLWhitespace(sqlparser.Stringify(parsed)), true, "applied"
}

func normalizeWarmupSQLWhitespace(SQL string) string {
	var builder strings.Builder
	builder.Grow(len(SQL))

	pendingSpace := false
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := 0; i < len(SQL); {
		r, size := utf8.DecodeRuneInString(SQL[i:])
		switch r {
		case '\'':
			if !inDoubleQuote && !inBacktick && !isEscapedSQLQuote(SQL, i) {
				inSingleQuote = !inSingleQuote
			}
			if pendingSpace && builder.Len() > 0 {
				builder.WriteByte(' ')
			}
			builder.WriteRune(r)
			pendingSpace = false
			i += size
		case '"':
			if !inSingleQuote && !inBacktick && !isEscapedSQLQuote(SQL, i) {
				inDoubleQuote = !inDoubleQuote
			}
			if pendingSpace && builder.Len() > 0 {
				builder.WriteByte(' ')
			}
			builder.WriteRune(r)
			pendingSpace = false
			i += size
		case '`':
			if !inSingleQuote && !inDoubleQuote {
				inBacktick = !inBacktick
			}
			if pendingSpace && builder.Len() > 0 {
				builder.WriteByte(' ')
			}
			builder.WriteRune(r)
			pendingSpace = false
			i += size
		default:
			if inSingleQuote || inDoubleQuote || inBacktick {
				builder.WriteRune(r)
				pendingSpace = false
				i += size
				continue
			}
			if unicode.IsSpace(r) {
				pendingSpace = builder.Len() > 0
				i += size
				continue
			}
			if operator, width, ok := warmupSQLOperator(SQL[i:]); ok {
				writeCanonicalWarmupOperator(&builder, &pendingSpace, operator)
				i += width
				continue
			}
			switch r {
			case ')', ']', ',':
				trimTrailingBuilderSpace(&builder)
			case '(':
				trimTrailingBuilderSpace(&builder)
			case '.':
				trimTrailingBuilderSpace(&builder)
			default:
				if pendingSpace && builder.Len() > 0 {
					builder.WriteByte(' ')
				}
			}
			builder.WriteRune(r)
			pendingSpace = false
			i += size
		}
	}

	return strings.TrimSpace(builder.String())
}

func warmupSQLOperator(SQL string) (string, int, bool) {
	switch {
	case strings.HasPrefix(SQL, "<="), strings.HasPrefix(SQL, ">="), strings.HasPrefix(SQL, "!="), strings.HasPrefix(SQL, "<>"):
		return SQL[:2], 2, true
	case strings.HasPrefix(SQL, "="), strings.HasPrefix(SQL, "<"), strings.HasPrefix(SQL, ">"):
		return SQL[:1], 1, true
	default:
		return "", 0, false
	}
}

func writeCanonicalWarmupOperator(builder *strings.Builder, pendingSpace *bool, operator string) {
	trimTrailingBuilderSpace(builder)
	if builder.Len() > 0 {
		text := builder.String()
		if text[len(text)-1] != ' ' {
			builder.WriteByte(' ')
		}
	}
	builder.WriteString(operator)
	*pendingSpace = true
}

func trimTrailingBuilderSpace(builder *strings.Builder) {
	text := builder.String()
	if len(text) == 0 || text[len(text)-1] != ' ' {
		return
	}
	builder.Reset()
	builder.WriteString(text[:len(text)-1])
}

func isEscapedSQLQuote(SQL string, index int) bool {
	if index == 0 {
		return false
	}
	return SQL[index-1] == '\\'
}

func (a *Cache) resolveIndexIdentity(SQL string, args []interface{}, options ...interface{}) (string, []interface{}, []byte, cache.WarmupIdentityMeta, error) {
	for _, option := range options {
		matcher, ok := option.(*cache.ParmetrizedQuery)
		if !ok || matcher == nil {
			continue
		}
		return matcher.WarmupIdentityResolved()
	}

	argsMarshal, err := json.Marshal(args)
	if err != nil {
		return "", nil, nil, cache.WarmupIdentityMeta{}, err
	}
	return SQL, args, argsMarshal, cache.WarmupIdentityMeta{Source: "execution", Detail: "default_execution_identity"}, nil
}

func (a *Cache) logWarmupMarker(stage string, column string, warmupURL string, markerKey string) {
	a.logWarmupf("aerospike cache %s set=%s column=%s warmup_url=%s marker_key=%s\n", stage, a.set, column, warmupURL, markerKey)
}

func (a *Cache) logWarmupIdentityResolved(stage string, by string, warmupURL string, SQL string, argsMarshal []byte, canonicalization string, meta cache.WarmupIdentityMeta) {
	a.logWarmupf("aerospike cache warmup_identity_resolved set=%s stage=%s by=%s source=%s detail=%s canonicalization=%s warmup_url=%s args_json=%q sql=%q\n", a.set, stage, by, meta.Source, meta.Detail, canonicalization, warmupURL, string(argsMarshal), SQL)
}

func (a *Cache) logWarmupRead(warmupURL string, markerKey string, found bool, err error) {
	by, _, _ := strings.Cut(markerKey, "#")
	if err != nil && !a.isKeyNotFoundErr(err) {
		a.logWarmupf("aerospike cache warmup_read set=%s by=%s warmup_url=%s marker_key=%s getRecord_found=%t err=%v\n", a.set, by, warmupURL, markerKey, found, err)
		return
	}
	a.logWarmupf("aerospike cache warmup_read set=%s by=%s warmup_url=%s marker_key=%s getRecord_found=%t\n", a.set, by, warmupURL, markerKey, found)
}

func (a *Cache) logWarmupFailure(by string, warmupURL string, markerKey string, failure string) {
	a.logWarmupf("aerospike cache warmup_failure set=%s by=%s warmup_url=%s marker_key=%s failure=%s\n", a.set, by, warmupURL, markerKey, failure)
}

func (a *Cache) logWarmupf(format string, args ...interface{}) {
	if !warmupDebugEnabled() {
		return
	}
	fmt.Printf(format, args...)
}

func warmupDebugEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SQLX_AEROSPIKE_WARMUP_DEBUG"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (a *Cache) updateLazyMatchEntry(ctx context.Context, anEntry *cache.Entry, match *RecordMatched, SQL string, jsonEncodedArgs []byte, stats *cache.Stats) error {
	if match == nil || !match.hasKey {
		return nil
	}

	if !a.recordMatches(match.record, SQL, jsonEncodedArgs) {
		if match.record != nil {
			_ = a.Delete(ctx, anEntry)
		}

		return nil
	}

	reader, err := a.reader(match.key, match.record)
	if err != nil {
		return err
	}

	anEntry.SetReader(reader, reader)

	stats.Type = cache.TypeReadSingle
	stats.RecordsCounter = 1
	stats.Key = match.keyValue
	if match.key != nil {
		stats.Dataset = match.key.SetName()
		stats.Namespace = match.key.Namespace()
	}
	return nil
}

func (a *Cache) updateColumnsInMatchEntry(entry *cache.Entry, match *RecordMatched, matcher *cache.ParmetrizedQuery, stats *cache.Stats) error {
	if matcher == nil || entry.ReadCloser != nil {
		return nil
	}

	identitySQL, _, identityArgsMarshal, err := matcher.WarmupIdentity()
	if err != nil {
		return err
	}
	identitySQL, identityArgsMarshal, _ = canonicalWarmupIdentity(identitySQL, identityArgsMarshal)

	if match == nil || !match.hasKey {
		warmupURL, markerKey := "", ""
		if identityURL, err := a.identityURL(identitySQL, nil, identityArgsMarshal); err == nil {
			warmupURL = identityURL
			markerKey = a.columnURL(identityURL, matcher.By)
		}
		a.logWarmupFailure(matcher.By, warmupURL, markerKey, "marker_miss")
		return nil
	}

	markerKey := match.keyValue
	warmupURL := match.baseURL
	if !a.recordMatches(match.record, identitySQL, identityArgsMarshal) {
		a.logWarmupFailure(matcher.By, warmupURL, markerKey, "marker_identity_mismatch")
		return nil
	}

	multiReader := NewMultiReader(matcher)

	chanSize := len(matcher.In)

	readerChan := make(chan *readerWrapper, chanSize)
	if chanSize == 0 {
		close(readerChan)
	}

	for i := range matcher.In {
		a.readChan(readerChan, matcher, warmupURL, matcher.In[i])
	}

	counter := 0
	childRowMiss := false
	for reader := range readerChan {
		if reader.err != nil {
			return reader.err
		}

		if reader.reader != nil {
			multiReader.AddReader(reader.reader)
		} else {
			childRowMiss = true
		}

		counter++
		if counter == chanSize {
			close(readerChan)
		}
	}
	if childRowMiss {
		a.logWarmupFailure(matcher.By, warmupURL, markerKey, "child_row_miss")
	}

	entry.SetReader(multiReader, multiReader)

	stats.Type = cache.TypeReadMulti
	stats.RecordsCounter = counter
	stats.Key = match.keyValue
	return nil
}

func (a *Cache) updateWriter(anEntry *cache.Entry, fullMatch *RecordMatched, SQL string, argsMarshal []byte, stats *cache.Stats) error {
	if anEntry.ReadCloser != nil {
		return nil
	}

	anEntry.Id += uuid.New().String()
	writer := a.newWriter(fullMatch.key, fullMatch.keyValue, SQL, argsMarshal)
	anEntry.SetWriter(writer, writer)
	writer.entry = anEntry
	stats.Key = fullMatch.keyValue
	if fullMatch.key != nil {
		stats.Dataset = fullMatch.key.SetName()
		stats.Namespace = fullMatch.key.Namespace()
	}
	stats.Type = cache.TypeWrite
	if anEntry.Meta.ExpiryTimeMs > 0 {
		expiresAt := time.UnixMilli(int64(anEntry.Meta.ExpiryTimeMs))
		stats.ExpiryTime = &expiresAt
	}
	return nil
}

func (a *Cache) readChan(readerChan chan *readerWrapper, matcher *cache.ParmetrizedQuery, warmupURL string, columnValue interface{}) {
	go func(matcher *cache.ParmetrizedQuery, warmupURL string, columnValue interface{}) {
		reader, err := a.newReader(matcher, warmupURL, columnValue)
		readerChan <- &readerWrapper{
			err:    err,
			reader: reader,
		}
	}(matcher, warmupURL, columnValue)
}

func (a *Cache) newReader(matcher *cache.ParmetrizedQuery, warmupURL string, columnValue interface{}) (*Reader, error) {
	valueMarshal, err := json.Marshal(columnValue)
	if err != nil {
		return nil, err
	}

	actualKeyValue := warmupURL
	if actualKeyValue == "" {
		identitySQL, identityArgs, args, err := matcher.WarmupIdentity()
		if err != nil {
			return nil, err
		}
		identitySQL, args, _ = canonicalWarmupIdentity(identitySQL, args)
		actualKeyValue, err = a.identityURL(identitySQL, identityArgs, args)
		if err != nil {
			return nil, err
		}
	}

	actualKeyValue = a.columnValueURL(matcher.By, valueMarshal, actualKeyValue)
	aKey, err := a.key(actualKeyValue)
	if err != nil {
		return nil, err
	}

	record, err := a.getRecord(aKey, cachedBins...)
	if a.isKeyNotFoundErr(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return a.reader(aKey, record)
}

func (a *Cache) isKeyNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	aeroErr, ok := err.(types.AerospikeError)
	if !ok {
		return false
	}

	code := aeroErr.ResultCode()
	return code == types.KEY_NOT_FOUND_ERROR
}

func asAerospikeErr(err error) (types.AerospikeError, bool) {
	if err == nil {
		return types.AerospikeError{}, false
	}

	aeroErr, ok := err.(types.AerospikeError)
	if !ok {
		return types.AerospikeError{}, false
	}

	return aeroErr, true
}

func (a *Cache) entryId(fullMatch *RecordMatched, columnsInMatch *RecordMatched) string {
	if fullMatch != nil {
		return fullMatch.keyValue
	}

	if columnsInMatch != nil {
		return columnsInMatch.keyValue
	}

	return ""
}

func (a *Cache) updateMetaFields(entry *cache.Entry, match *RecordMatched, columnsInMatch *RecordMatched) error {
	var record *as.Record
	if match != nil {
		record = match.record
	}

	if record == nil && columnsInMatch != nil {
		record = columnsInMatch.record
	}

	if record == nil {
		return nil
	}

	cacheFields := record.Bins[fieldsBin]
	if cacheFields == nil {
		return fmt.Errorf("not found %v bin in cache entry ", fieldsBin)
	}

	fieldsStr, ok := cacheFields.(string)
	if !ok {
		return fmt.Errorf("expected fields to be type of %T but got %T", fieldsStr, cacheFields)
	}

	if err := json.Unmarshal([]byte(fieldsStr), &entry.Meta.Fields); err != nil {
		return err
	}

	for _, field := range entry.Meta.Fields {
		if err := field.Init(); err != nil {
			return err
		}
	}

	return nil
}

func (a *Cache) fetchAndIndexValues(ctx context.Context, fields []*cache.Field, column string, rows *sql.Rows, dest chan *cache.Indexed, ordered bool) error {
	const (
		indexProgressInterval = 30 * time.Second
		indexProgressRowStep  = 500000
	)

	indexSource, err := NewIndexSource(column, ordered, fields, dest)
	if err != nil {
		return err
	}

	started := time.Now()
	lastProgress := started
	lastProgressRows := 0
	processed := 0
	columnIndex := indexSource.ColumnIndex()
	placeholders := NewPlaceholders(columnIndex, fields)

	for rows.Next() {
		processed++
		if err = rows.Scan(placeholders.ScanPlaceholders()...); err != nil {
			return err
		}

		columnValue, ok := placeholders.ColumnValue()
		if !ok {
			continue
		}

		indexed := indexSource.Index(columnValue)
		indexed.Column = column

		if err = indexed.StringifyData(placeholders.Values()); err != nil {
			return err
		}

		if time.Since(lastProgress) >= indexProgressInterval && processed-lastProgressRows >= indexProgressRowStep {
			cache.EmitIndexProgress(ctx, &cache.IndexProgressEvent{
				Column:  column,
				Rows:    processed,
				Elapsed: time.Since(started),
			})
			lastProgress = time.Now()
			lastProgressRows = processed
		}
	}

	if err = indexSource.Close(); err != nil {
		return err
	}
	cache.EmitIndexProgress(ctx, &cache.IndexProgressEvent{
		Column:  column,
		Rows:    processed,
		Elapsed: time.Since(started),
		Done:    true,
	})
	return nil
}

func (a *Cache) handleResponseFailure(code types.ResultCode) {
	if a.failureHandler == nil {
		return
	}

	if code == types.OK {
		a.failureHandler.HandleSuccess()
	} else {
		a.failureHandler.HandleFailure()
	}
}

func (a *Cache) getRecord(key *as.Key, bins ...string) (*as.Record, error) {
	record, err := a.client.Get(a.newBasePolicy(true), key, bins...)
	if err != nil {
		aerospikeErr, ok := asAerospikeErr(err)
		if ok {
			a.handleResponseFailure(aerospikeErr.ResultCode())
		}

		return nil, err
	}

	return record, nil
}

func (a *Cache) put(key *as.Key, binMap as.BinMap) error {
	policy := a.writePolicy()
	err := a.client.Put(policy, key, binMap)
	aerospikeErr, ok := asAerospikeErr(err)
	if ok {
		a.handleResponseFailure(aerospikeErr.ResultCode())
	}

	return err
}

func New(namespace string, setName string, client *as.Client, timeToLiveInSec uint32, options ...interface{}) (*Cache, error) {
	var recorder cache.Recorder
	var allowSmart bool
	var timeoutConfig *TimeoutConfig
	var globalFailureHandler *FailureHandler

	for _, anOption := range options {
		switch actual := anOption.(type) {
		case cache.Recorder:
			recorder = actual
		case cache.AllowSmart:
			allowSmart = bool(actual)
		case *TimeoutConfig:
			timeoutConfig = actual
		case *FailureHandler:
			globalFailureHandler = actual
		}
	}

	return &Cache{
		client:          client,
		namespace:       namespace,
		set:             setName,
		recorder:        recorder,
		timeToLiveInSec: timeToLiveInSec,
		allowSmart:      allowSmart,
		timeoutConfig:   timeoutConfig,
		failureHandler:  globalFailureHandler,
	}, nil
}
