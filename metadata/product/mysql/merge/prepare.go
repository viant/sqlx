package merge

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/minio/highwayhash"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/moption"
	"github.com/viant/sqlx/shared"
	"strings"
	"sync"
	"time"
)

func (e *Executor) prepareDMLDataSetsInsUpdDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, allInSrcCnt int) ([]interface{}, []interface{}, []interface{}, error) {
	srcRowsByKey, srcRowsIdsByKey, err := e.index(allInSrcCnt, valueAt, true)
	if err != nil {
		return nil, nil, nil, err
	}
	indexedSrcCnt := len(srcRowsByKey)

	dstDataReader, err := read.New(ctx, db, e.config.FetchSQL, e.config.NewRowFn)
	if err != nil {
		return nil, nil, nil, err
	}

	justInDstByKeyAndId, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey, allInDstCnt, err := e.prepareDataSets(ctx, dstDataReader, srcRowsByKey, srcRowsIdsByKey)
	justInSrcByKeyAndId := srcRowsByKey
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec: failed to fetch target data due to: %w", err)
	}
	e.fillMetricSrcDstComparison(allInSrcCnt, indexedSrcCnt, allInDstCnt, justInSrcByKeyAndId, justInDstByKeyAndId, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey)

	dataToInsert, dataToUpdate, dataToDelete := e.categorize(justInDstByKeyAndId, justInSrcByKeyAndId, inSrcAndDstByIdButNotByKey)
	e.fillMetricInsUpdDelSetsSummary(dataToInsert, dataToUpdate, dataToDelete)

	return dataToInsert, dataToUpdate, dataToDelete, err
}

func (e *Executor) prepareDMLDataSetsInsDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, rawSrcRowCnt int, tableName string) ([]interface{}, []interface{}, []interface{}, error) {
	fName := "preparedmldatasetsinsdel"

	allSrcHashToIdxMap, allSrcHashToIdxMapCnt, err := e.indexFast(rawSrcRowCnt, valueAt, false)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec %s: failed to index src data due to: %w", fName, err)
	}

	onlyInDst, srcExistsInBoth, allInDstCnt, onlyInSrcCnt, inBothCnt, err := e.prepareDataSetsInsDel(ctx, allSrcHashToIdxMap, allSrcHashToIdxMapCnt, rawSrcRowCnt, db, tableName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec %s: failed to prepare data sets (ins, del) due to: %w", fName, err)
	}
	e.fillMetricSrcDstComp(rawSrcRowCnt, allSrcHashToIdxMapCnt, allInDstCnt, onlyInSrcCnt, len(onlyInDst), inBothCnt, 0)

	dataToInsert, dataToUpdate, dataToDelete, err := e.categorizeInsDel(onlyInDst, allSrcHashToIdxMap, onlyInSrcCnt, valueAt, srcExistsInBoth)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec %s: failed to categorize data sets (ins, del) due to: %w", fName, err)
	}
	e.fillMetricInsUpdDelSetsSummary(dataToInsert, dataToUpdate, dataToDelete)

	return dataToInsert, dataToUpdate, dataToDelete, nil
}

func (e *Executor) prepareDMLDataSetsUpsDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, allInSrcCnt int) ([]interface{}, []interface{}, []interface{}, error) {
	srcRowsByKey, srcRowsIdsByKey, err := e.index(allInSrcCnt, valueAt, true)
	if err != nil {
		return nil, nil, nil, err
	}
	indexedSrcCnt := len(srcRowsByKey)

	dstDataReader, err := read.New(ctx, db, e.config.FetchSQL, e.config.NewRowFn)
	if err != nil {
		return nil, nil, nil, err
	}

	justInDstByKeyAndId, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey, allInDstCnt, err := e.prepareDataSets(ctx, dstDataReader, srcRowsByKey, srcRowsIdsByKey)
	justInSrcByKeyAndId := srcRowsByKey
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec: failed to fetch target data due to: %w", err)
	}
	e.fillMetricSrcDstComparison(allInSrcCnt, indexedSrcCnt, allInDstCnt, justInSrcByKeyAndId, justInDstByKeyAndId, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey)

	dataToInsert, dataToUpdate, dataToDelete := e.categorizeUpsDel(justInDstByKeyAndId, justInSrcByKeyAndId, inSrcAndDstByIdButNotByKey)
	e.fillMetricUpsDelSetsSummary(dataToInsert, dataToUpdate, dataToDelete)

	return dataToInsert, dataToUpdate, dataToDelete, err
}

// TODO handle case with no PK
func (e *Executor) prepareDataSets(ctx context.Context, dstDataReader *read.Reader, justInSrcByKey map[interface{}]interface{}, justInSrcIdsKeys map[interface{}]interface{}) (map[interface{}]interface{}, []interface{}, []interface{}, int, error) {
	start := time.Now()
	defer func() {
		e.metric.FetchTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# FETCHING AND COMPARE TIME: %s\n", e.metric.FetchTime))
	}()

	var inSrcAndDstByKey = make([]interface{}, 0)
	var inSrcAndDstByIdButNotByKey = make([]interface{}, 0)

	var justInDstByKey = map[interface{}]interface{}{}
	var justInSrcKeysIds = map[interface{}]interface{}{}

	allInDstCnt := 0

	err := dstDataReader.QueryAll(ctx, func(row interface{}) error {
		key, identity, err := e.config.MatchKeyFn(row)
		if err != nil {
			return err
		}

		if _, ok := justInSrcByKey[key]; ok { // row with match key is present in target table and source data
			delete(justInSrcByKey, key)
			delete(justInSrcIdsKeys, key) // only deleting by key, because src id can be different
			inSrcAndDstByKey = append(inSrcAndDstByKey, row)
		} else { // row with match key is present in target table but is not present in source data
			justInDstByKey[identity] = row
		}

		allInDstCnt++
		return nil
	})

	if err != nil {
		return nil, nil, nil, 0, err
	}

	inSrcAndDstIds, _, _ := prepareIdsSets(justInSrcIdsKeys, justInDstByKey)

	for k, v := range justInSrcIdsKeys {
		justInSrcKeysIds[v] = k
	}

	for _, id := range inSrcAndDstIds {
		inSrcAndDstByIdButNotByKey = append(inSrcAndDstByIdButNotByKey, justInSrcByKey[justInSrcKeysIds[id]])
		delete(justInSrcByKey, justInSrcKeysIds[id])
		delete(justInDstByKey, id)
	}

	justInDstByKeyAndId := justInDstByKey
	return justInDstByKeyAndId, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey, allInDstCnt, nil
}

func (e *Executor) prepareDataSetsInsDel(ctx context.Context, allSrcHashToIdxMap interface{}, allSrcHashToIdxMapCnt int, allRawSrcCnt int, db *sql.DB, tableName string) (
	[]interface{}, []bool, int, int, int, error) {

	defer func() {
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# FETCHING DATA TIME: %s\n", e.metric.FetchTime))
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# COMPARING DATA TIME: %s\n", e.metric.CompareDataTime))
	}()

	startFetch := time.Now()
	allDstRows, err := e.fetchData(ctx, db, tableName)
	e.metric.FetchTime = time.Now().Sub(startFetch)
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}

	startCompare := time.Now()
	srcExistsInBoth, onlyInDst, inBothCnt, onlyInSrcCnt, err := e.compare(allRawSrcCnt, allSrcHashToIdxMap, allDstRows, allSrcHashToIdxMapCnt)
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}
	e.metric.CompareDataTime = time.Now().Sub(startCompare)

	return onlyInDst, srcExistsInBoth, len(allDstRows), onlyInSrcCnt, inBothCnt, nil
}

func (e *Executor) compare(allRawSrcCnt int, allSrcHashToIdxMap interface{}, allDstRows []interface{}, allSrcHashToIdxMapCnt int) ([]bool, []interface{}, int, int, error) {
	dstExistsOnlyInDst, srcExistsInBoth, inBothPartialCounts, onlyInDstCounts, err := e.compareData(allRawSrcCnt, allSrcHashToIdxMap, allDstRows)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	onlyInDst := e.getOnlyInDst(onlyInDstCounts, dstExistsOnlyInDst, allDstRows)

	inBothCnt, onlyInSrcCnt, err := e.total(inBothPartialCounts, allSrcHashToIdxMapCnt)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	return srcExistsInBoth, onlyInDst, inBothCnt, onlyInSrcCnt, nil
}

func (e *Executor) total(inSrcAndDstByKeyCounters []int, allSrcRowIdxByKeyCnt int) (int, int, error) {
	fName := "total"
	inSrcAndDstByKeyCnt := 0
	for _, c := range inSrcAndDstByKeyCounters {
		inSrcAndDstByKeyCnt += c
	}

	onlyInSrcByKeyCnt := allSrcRowIdxByKeyCnt - inSrcAndDstByKeyCnt
	if onlyInSrcByKeyCnt < 0 {
		err := fmt.Errorf("%s - the number of rows occurring only in the source was less than 0", fName)
		return 0, 0, err
	}
	return inSrcAndDstByKeyCnt, onlyInSrcByKeyCnt, nil
}

func (e *Executor) getOnlyInDst(onlyInDstCounts []int, dstExistsOnlyInDst []bool, allDstRows []interface{}) []interface{} {
	onlyInDstCnt := 0
	for _, x := range onlyInDstCounts {
		onlyInDstCnt += x
	}

	onlyInDst := make([]interface{}, onlyInDstCnt)
	k := 0
	for i, exists := range dstExistsOnlyInDst {
		if exists {
			onlyInDst[k] = allDstRows[i]
			k++
		}
	}
	return onlyInDst
}

func (e *Executor) compareData(rawSrcRowCnt int, allSrcHashToIdxMap interface{}, allDstRows []interface{}) ([]bool, []bool, []int, []int, error) {
	fName := "comparedata"

	var (
		errors             = &shared.Errors{}
		wg                 = &sync.WaitGroup{}
		allInDstCnt        = len(allDstRows)
		dstExistsOnlyInDst = make([]bool, allInDstCnt)
		srcExistsInBoth    = make([]bool, rawSrcRowCnt)
		routineCnt         = e.config.CompareConcurrency
	)

	if routineCnt < 1 {
		routineCnt = 1
	}

	chunkSize := allInDstCnt / routineCnt

	inBothCounts := make([]int, routineCnt)
	onlyInDstCounts := make([]int, routineCnt)

	for n := 0; n < routineCnt; n++ {
		begin := n * chunkSize
		end := (n + 1) * chunkSize
		if n == routineCnt-1 {
			end = allInDstCnt
		}

		switch len(e.hashKey) {
		case 0:
			aAllSrcHashToIdxMap, ok := allSrcHashToIdxMap.(map[interface{}]int)
			if !ok {
				errors.Add(fmt.Errorf("%s - invalid type, expected: %T got %T", fName, aAllSrcHashToIdxMap, allSrcHashToIdxMap))
				continue
			}

			wg.Add(1)
			go e.compareWithKey(begin, end, allDstRows, wg, aAllSrcHashToIdxMap,
				srcExistsInBoth, dstExistsOnlyInDst, &inBothCounts[n], &onlyInDstCounts[n], errors)
		default:
			aAllSrcHashToIdxMap, ok := allSrcHashToIdxMap.(map[uint64]int)
			if !ok {
				errors.Add(fmt.Errorf("%s - invalid type, expected: %T got %T", fName, aAllSrcHashToIdxMap, allSrcHashToIdxMap))
				continue
			}

			wg.Add(1)
			go e.compareWithKeyAndHash(begin, end, allDstRows, wg, aAllSrcHashToIdxMap,
				srcExistsInBoth, dstExistsOnlyInDst, &inBothCounts[n], &onlyInDstCounts[n], errors)
		}
	}

	wg.Wait()
	if err := errors.First(); err != nil {
		return nil, nil, nil, nil, err
	}

	return dstExistsOnlyInDst, srcExistsInBoth, inBothCounts, onlyInDstCounts, nil
}

func (e *Executor) fetchData(ctx context.Context, db *sql.DB, tableName string) ([]interface{}, error) {
	if e.config.FetchConcurrency < 2 {
		return e.fetchDataSequential(ctx, db)
	} else {
		return e.fetchDataConcurrent(ctx, db, tableName, e.config.FetchConcurrency)
	}
}

func (e *Executor) fetchDataSequential(ctx context.Context, db *sql.DB) ([]interface{}, error) {
	dstDataReader, err := read.New(ctx, db, e.config.FetchSQL, e.config.NewRowFn)
	if err != nil {
		return nil, err
	}

	allRows := make([]interface{}, 0, 1000)
	err = dstDataReader.QueryAll(ctx, func(row interface{}) error {
		allRows = append(allRows, row)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return allRows, nil
}

func (e *Executor) fetchDataConcurrent(ctx context.Context, db *sql.DB, tableName string, bucketCnt int) ([]interface{}, error) {
	start := time.Now()
	errors := &shared.Errors{}

	ids, err := e.getIds(db, tableName)
	if err != nil {
		return nil, err
	}

	count := len(ids)
	allRows := make([]interface{}, count)
	overflow := make([][]interface{}, bucketCnt)
	missing := make([][]int, bucketCnt)
	wg := sync.WaitGroup{}

	if bucketCnt < 1 {
		bucketCnt = 1
	}
	bucketSize := count / bucketCnt

	for n := 0; n < bucketCnt; n++ {
		begin := n * bucketSize
		end := (n+1)*bucketSize - 1
		if n == bucketCnt-1 {
			end = count - 1
		}

		if end < 0 {
			continue
		}

		wg.Add(1)
		go func(begin, end, bucketIdx int) {
			defer wg.Done()
			//TODO consider config in future
			query := fmt.Sprintf("%s WHERE ID BETWEEN %d AND %d", e.config.FetchSQL, ids[begin], ids[end])

			partDataReader, err := read.New(ctx, db, query, e.config.NewRowFn)
			if err != nil {
				errors.Add(err)
				return
			}

			partIdx := 0
			err = partDataReader.QueryAll(ctx, func(row interface{}) error {
				// in case of data change in db between queries: select count(*) and select *
				if partIdx > end-begin {
					overflow[bucketIdx] = append(overflow[bucketIdx], row)
					return nil
				}

				allRows[begin+partIdx] = row
				partIdx++
				return nil
			})

			// in case of data change in db between queries: select count(*) and select *
			if partIdx <= end-begin {
				missing[bucketIdx] = []int{begin + partIdx, end}
			}

			if err != nil {
				errors.Add(err)
				return
			}

		}(begin, end, n)
	}

	wg.Wait()

	if err := errors.First(); err != nil {
		return nil, err
	}

	// repair is needed in case of data change in db between queries: select count(*) and select *
	allRows = e.repairIfNeeded(missing, overflow, allRows, count)
	e.metric.FetchTime = time.Now().Sub(start)

	return allRows, nil
}

func (e *Executor) getIds(db *sql.DB, tableName string) (result []int, err error) {
	start := time.Now()
	result = make([]int, 0, 1000)

	defer func() {
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# FETCHING DATA - GETTING IDS SUBTIME: %s\n", time.Now().Sub(start)))
	}()

	//TODO consider config in future
	rows, err := db.Query(fmt.Sprintf("SELECT ID FROM %s ORDER BY ID", tableName))
	if err != nil {
		return nil, err
	}

	defer func() {
		if err2 := rows.Close(); err2 != nil {
			if err == nil {
				err = err2
			} else {
				err = fmt.Errorf("%v; %v", err, err2)
			}
		}
	}()

	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}

	return result, err
}

func (e *Executor) repairIfNeeded(missing [][]int, overflow [][]interface{}, allRows []interface{}, count int) []interface{} {
	foundMissing := false
	foundOverflow := false
	missedCnt := 0
	overflowCnt := 0

	for _, v := range missing {
		if len(v) == 2 {
			foundMissing = true
			missedCnt += v[1] - v[0] + 1
		}
	}

	for _, v := range overflow {
		if len(v) > 0 {
			foundOverflow = true
			overflowCnt += len(v)
		}
	}

	if !foundMissing && !foundOverflow {
		return allRows
	}

	repaired := make([]interface{}, 0, count-missedCnt+overflowCnt)

	if foundMissing {
		begin := 0
		end := 0

		for _, v := range missing {
			if len(v) == 2 {
				end = v[0]
				repaired = append(repaired, allRows[begin:end]...)
				begin = v[1] + 1
			}
		}

		if begin < count {
			repaired = append(repaired, allRows[begin:]...)
		}
	}

	if foundOverflow {
		for _, o := range overflow {
			repaired = append(repaired, o...)
		}
	}

	return repaired
}

func (e *Executor) categorize(justInDstByKeyAndId map[interface{}]interface{}, justInSrcByKeyAndId map[interface{}]interface{}, inSrcAndDstByIdButNotByKey []interface{}) ([]interface{}, []interface{}, []interface{}) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING (categorize) INS/UPD/DEL SETS time: %s\n", e.metric.CategorizeTime))
	}()

	dataToInsert := make([]interface{}, len(justInSrcByKeyAndId))
	dataToUpdate := inSrcAndDstByIdButNotByKey
	dataToDelete := make([]interface{}, len(justInDstByKeyAndId))

	i := 0
	for _, record := range justInDstByKeyAndId {
		dataToDelete[i] = record
		i++
	}

	i = 0
	for _, record := range justInSrcByKeyAndId {
		dataToInsert[i] = record
		i++
	}
	return dataToInsert, dataToUpdate, dataToDelete
}

func (e *Executor) categorizeInsDel(onlyInDst []interface{}, onlyInSrc interface{}, onlyInSrcCnt int, valueAt io.ValueAccessor, srcExistsInBoth []bool) ([]interface{}, []interface{}, []interface{}, error) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING INS/UPD/DEL SETS time: %s\n", e.metric.CategorizeTime))
	}()

	dataToInsert := make([]interface{}, onlyInSrcCnt)
	dataToUpdate := make([]interface{}, 0)
	dataToDelete := onlyInDst

	i := 0
	switch onlyInSrc.(type) {
	case map[uint64]int:
		for _, index := range onlyInSrc.(map[uint64]int) {
			if srcExistsInBoth[index] {
				continue
			}
			dataToInsert[i] = valueAt(index)
			i++
		}
	case map[interface{}]int:
		for _, index := range onlyInSrc.(map[interface{}]int) {
			if srcExistsInBoth[index] {
				continue
			}
			dataToInsert[i] = valueAt(index)
			i++
		}
	default:
		return nil, nil, nil, fmt.Errorf("categorizeinsdel - unsupported type %T", onlyInSrc)
	}

	return dataToInsert, dataToUpdate, dataToDelete, nil
}

func (e *Executor) categorizeUpsDel(justInDstByKeyAndId map[interface{}]interface{}, justInSrcByKeyAndId map[interface{}]interface{}, inSrcAndDstByIdButNotByKey []interface{}) ([]interface{}, []interface{}, []interface{}) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING INS/UPD/DEL SETS time: %s\n", e.metric.CategorizeTime))
	}()

	dataToInsert := make([]interface{}, len(justInSrcByKeyAndId)+len(inSrcAndDstByIdButNotByKey))
	dataToUpdate := []interface{}{}
	dataToDelete := make([]interface{}, len(justInDstByKeyAndId))

	i := 0
	for _, record := range justInDstByKeyAndId {
		dataToDelete[i] = record
		i++
	}

	i = 0
	for _, record := range justInSrcByKeyAndId {
		dataToInsert[i] = record
		i++
	}

	for _, record := range inSrcAndDstByIdButNotByKey {
		dataToInsert[i] = record
		i++
	}

	return dataToInsert, dataToUpdate, dataToDelete
}

func prepareIdsSets(a map[interface{}]interface{}, b map[interface{}]interface{}) (inAAndB, inAButNotB, inBButNotA []interface{}) {

	m := make(map[interface{}]uint8)
	for _, id := range a {
		m[id] |= 1
	}

	for id := range b {
		m[id] |= 2
	}

	for k, v := range m {
		a := v&1 != 0
		b := v&2 != 0
		switch {
		case a && b:
			inAAndB = append(inAAndB, k)
		case a && !b:
			inAButNotB = append(inAButNotB, k)
		case !a && b:
			inBButNotA = append(inBButNotA, k)
		}
	}

	return inAAndB, inAButNotB, inBButNotA
}

func (e *Executor) index(srcCnt int, valueAt io.ValueAccessor, withIds bool) (map[interface{}]interface{}, map[interface{}]interface{}, error) {
	start := time.Now()
	defer func() {
		e.metric.IndexTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# INDEXING TIME: %s\n", e.metric.IndexTime))
	}()

	var srcRowsByKey = make(map[interface{}]interface{})
	var srcRowsIdsByKey = make(map[interface{}]interface{})
	for i := 0; i < srcCnt; i++ {
		rec := valueAt(i)
		key, identity, err := e.config.MatchKeyFn(rec)
		if err != nil {
			return nil, nil, err
		}
		srcRowsByKey[key] = rec
		if withIds {
			srcRowsIdsByKey[key] = identity
		}
	}

	return srcRowsByKey, srcRowsIdsByKey, nil
}

func (e *Executor) indexFast(srcCnt int, valueAt io.ValueAccessor, withIds bool) (interface{}, int, error) {
	start := time.Now()
	defer func() {
		e.metric.IndexTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# INDEXING TIME: %s\n", e.metric.IndexTime))
	}()

	switch len(e.hashKey) {
	case 0:
		return e.indexWithKey(srcCnt, valueAt)
	default:
		return e.indexWithKeyAndHash(srcCnt, valueAt)
	}
}

func (e *Executor) indexWithKey(srcCnt int, valueAt io.ValueAccessor) (interface{}, int, error) {
	var result = make(map[interface{}]int)

	for i := 0; i < srcCnt; i++ {
		rec := valueAt(i)
		key, _ /*identity*/, err := e.config.MatchKeyFn(rec)
		if err != nil {
			return nil, 0, err
		}
		result[key] = i
	}

	return result, len(result), nil
}

func (e *Executor) indexWithKeyAndHash(srcCnt int, valueAt io.ValueAccessor) (interface{}, int, error) {
	var result = make(map[uint64]int)

	hash, err := highwayhash.New64(e.hashKey)
	if err != nil {
		return nil, 0, err
	}

	for i := 0; i < srcCnt; i++ {
		rec := valueAt(i)
		key, _ /*identity*/, err := e.config.MatchKeyFn(rec)
		if err != nil {
			return nil, 0, err
		}

		if i == 0 {
			aKey, ok := key.(string)
			if !ok {
				return nil, 0, fmt.Errorf("invalid type - expected %T but got %T", aKey, key)
			}
		}

		sKey, err := e.sum64(&hash, key.(string))
		if err != nil {
			return nil, 0, err
		}

		result[sKey] = i
	}

	return result, len(result), nil
}

func (e *Executor) ensureLoader(ctx context.Context, db *sql.DB, table string, options ...moption.Option) (*load.Service, error) {
	fnName := "ensureLoader"
	e.mux.Lock()
	defer e.mux.Unlock()

	var err error
	loader, ok := e.loaders[table]
	if !ok {
		if loader, err = load.New(ctx, db, table); err != nil {
			return nil, fmt.Errorf("%s.%s: failed to create loader for table %s due to: %w", packageName, fnName, table, err)
		}
		e.loaders[table] = loader
	}

	return loader, err
}

func (e *Executor) fillMetricSrcDstComparison(allInSrcCnt int, indexedSrcCnt int, allInDstCnt int, justInSrcByKeyAndId, justInDstByKeyAndId map[interface{}]interface{}, inSrcAndDstByKey, inSrcAndDstByIdButNotByKey []interface{}) {
	e.fillMetricSrcDstComp(allInSrcCnt, indexedSrcCnt, allInDstCnt, len(justInSrcByKeyAndId), len(justInDstByKeyAndId), len(inSrcAndDstByKey), len(inSrcAndDstByIdButNotByKey))
}

func (e *Executor) fillMetricSrcDstComparisonInsDel(allInSrcCnt, indexedSrcCnt, allInDstCnt int, justInSrcByKeyAndId map[interface{}]interface{}, justInDstByKeyAndId, inSrcAndDstByKey []interface{}) {
	e.fillMetricSrcDstComp(allInSrcCnt, indexedSrcCnt, allInDstCnt, len(justInSrcByKeyAndId), len(justInDstByKeyAndId), len(inSrcAndDstByKey), 0)
}

func (e *Executor) fillMetricSrcDstComparisonInsDelZ(allInSrcCnt, indexedSrcCnt, allInDstCnt int, justInSrcByKeyAndIdLen int, justInDstByKeyAndId []interface{}, inSrcAndDstByKeyCnt int) {
	e.fillMetricSrcDstComp(allInSrcCnt, indexedSrcCnt, allInDstCnt, justInSrcByKeyAndIdLen, len(justInDstByKeyAndId), inSrcAndDstByKeyCnt, 0)
}

func (e *Executor) fillMetricSrcDstComp(allInSrcCnt int, indexedSrcCnt int, allInDstCnt int, justInSrcByKeyAndIdCnt, justInDstByKeyAndIdCnt, inSrcAndDstByKeyCnt, inSrcAndDstByIdButNotByKeyCnt int) {
	e.metric.Strategy = e.config.Strategy
	e.metric.InSrcCnt = allInSrcCnt
	e.metric.InDstCnt = allInDstCnt
	e.metric.InSrcAndDstByKeyCnt = inSrcAndDstByKeyCnt
	e.metric.InSrcAndDstByIdButNotByKeyCnt = inSrcAndDstByIdButNotByKeyCnt
	e.metric.JustInSrcByKeyAndIdCnt = justInSrcByKeyAndIdCnt
	e.metric.JustInDstByKeyAndIdCnt = justInDstByKeyAndIdCnt

	sb := strings.Builder{}
	sb.WriteString("+++++++++++++++++++++++++++\n")
	sb.WriteString("          DATA SETS ROWS SUMMARY:\n")
	sb.WriteString(fmt.Sprintf("                   merge strategy: %s\n", info.MergeStrategyDesc(e.config.Strategy)))
	sb.WriteString(fmt.Sprintf("                  all rows in src: %d (%s)\n", allInSrcCnt, "raw data"))
	sb.WriteString(fmt.Sprintf("              indexed rows in src: %d (%s)\n", indexedSrcCnt, "unique by match key"))
	sb.WriteString(fmt.Sprintf("                  all rows in dst: %d\n", allInDstCnt))
	sb.WriteString(fmt.Sprintf("            in src and dst by key: %d (%d => src + dst)\n", inSrcAndDstByKeyCnt, 2*inSrcAndDstByKeyCnt))
	sb.WriteString(fmt.Sprintf("  in src and dst by id not by key: %d (%d => src + dst)\n", inSrcAndDstByIdButNotByKeyCnt, 2*inSrcAndDstByIdButNotByKeyCnt))
	sb.WriteString(fmt.Sprintf("        just in src by key and id: %d\n", justInSrcByKeyAndIdCnt))
	sb.WriteString(fmt.Sprintf("        just in dst by key and id: %d\n", justInDstByKeyAndIdCnt))
	sb.WriteString("+++++++++++++++++++++++++++\n")

	e.metric.Total.Report = append(e.metric.Total.Report, sb.String())
}

func (e *Executor) fillMetricInsUpdDelSetsSummary(dataToInsert []interface{}, dataToUpdate []interface{}, dataToDelete []interface{}) {
	e.fillMetricInsUpsUpdDelSetsSummary(len(dataToInsert), 0, len(dataToUpdate), len(dataToDelete))
}

func (e *Executor) fillMetricUpsDelSetsSummary(dataToInsert []interface{}, dataToUpdate []interface{}, dataToDelete []interface{}) {
	e.fillMetricInsUpsUpdDelSetsSummary(0, len(dataToInsert), len(dataToUpdate), len(dataToDelete))
}

func (e *Executor) fillMetricInsUpsUpdDelSetsSummary(toInsertCnt, toUpsertCnt, toUpdateCnt, toDeleteCnt int) {
	e.metric.ToInsertCnt = toInsertCnt
	e.metric.ToUpdateCnt = toUpdateCnt
	e.metric.ToUpsertCnt = toUpsertCnt
	e.metric.ToDeleteCnt = toDeleteCnt

	sb := strings.Builder{}
	sb.WriteString("********************************\n")
	sb.WriteString("INS, UPS, UPD, DEL SETS TOTAL:\n")
	sb.WriteString(fmt.Sprintf("rec to insert cnt: %d\n", toInsertCnt))
	sb.WriteString(fmt.Sprintf("rec to upsert cnt: %d\n", toUpsertCnt))
	sb.WriteString(fmt.Sprintf("rec to update cnt: %d\n", toUpdateCnt))
	sb.WriteString(fmt.Sprintf("rec to delete cnt: %d\n", toDeleteCnt))
	sb.WriteString("********************************\n")

	e.metric.Total.Report = append(e.metric.Total.Report, sb.String())
}

func (e *Executor) compareWithKeyAndHash(begin int, end int, allRows []interface{}, wg *sync.WaitGroup, allSrcRowIdxByKey map[uint64]int,
	srcExistsInBoth []bool, dstExistsOnlyInDst []bool, inBothCount *int, onlyInDstCount *int, errors *shared.Errors,
) {
	fName := "comparewithkeyandhash"
	defer wg.Done()
	hash, err := highwayhash.New64(e.hashKey)
	if err != nil {
		errors.Add(fmt.Errorf("%s %w", fName, err))
		return
	}

	for dstIdx := begin; dstIdx < end; dstIdx++ {
		row := allRows[dstIdx]
		key, _, err := e.config.MatchKeyFn(row)
		if err != nil {
			errors.Add(fmt.Errorf("%s: %w", fName, err))
			return
		}

		if dstIdx == begin {
			if test, ok := key.(string); !ok {
				errors.Add(fmt.Errorf("%s: invalid type - excepted %T, got %T", fName, test, key))
				return
			}
		}

		sKey, err := e.sum64(&hash, key.(string))
		if srcIdx, ok := allSrcRowIdxByKey[sKey]; ok { // row with match key is present in target table and source data
			if srcExistsInBoth[srcIdx] {
				errors.Add(fmt.Errorf("%s: matchkey duplicate detected in dst data key (from matchfn) %v hashkey %v", fName, key, sKey))
				return
			}
			srcExistsInBoth[srcIdx] = true
			*inBothCount++
		} else { // row with match key is present in target table but is not present in source data
			if dstExistsOnlyInDst[dstIdx] {
				errors.Add(fmt.Errorf("%s: matchkey duplicate detected in dst data key (from matchfn) %v hashkey %v dst index %v", fName, key, sKey, dstIdx))
				return
			}
			dstExistsOnlyInDst[dstIdx] = true
			*onlyInDstCount++
		}
	}
}

func (e *Executor) compareWithKey(begin int, end int, allDstRows []interface{}, wg *sync.WaitGroup, allSrcHashToIdxMap map[interface{}]int,
	srcExistsInBoth []bool, dstExistsOnlyInDst []bool, inBothCount *int, onlyInDstCount *int, errors *shared.Errors,
) {
	fName := "comparewithkey"
	defer wg.Done()

	for dstIdx := begin; dstIdx < end; dstIdx++ {
		row := allDstRows[dstIdx]
		key, _, err := e.config.MatchKeyFn(row)
		if err != nil {
			errors.Add(fmt.Errorf("%s: %w", fName, err))
			return
		}

		if srcIdx, ok := allSrcHashToIdxMap[key]; ok { // row with match key is present in target table and source data
			if srcExistsInBoth[srcIdx] {
				errors.Add(fmt.Errorf("%s: indexing src data: matchkey duplicate detected in dst data key (from matchfn) %v", fName, key))
				return
			}
			srcExistsInBoth[srcIdx] = true
			*inBothCount++
		} else { // row with match key is present in target table but is not present in source data
			if dstExistsOnlyInDst[dstIdx] {
				errors.Add(fmt.Errorf("%s: indexing dst data: matchkey duplicate detected in dst data key (from matchfn) %v hashkey", fName, key, dstIdx))
				return
			}
			dstExistsOnlyInDst[dstIdx] = true
			*onlyInDstCount++
		}
	}
}
