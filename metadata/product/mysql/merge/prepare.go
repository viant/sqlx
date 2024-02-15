package merge

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/moption"
	"strings"
	"time"
)

func (e *Executor) prepareDMLDataSetsInsUpdDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, allInSrcCnt int) ([]interface{}, []interface{}, []interface{}, error) {
	srcRowsByKey, srcRowsIdsByKey, err := e.index(allInSrcCnt, valueAt)
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

func (e *Executor) prepareDMLDataSetsInsDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, allInSrcCnt int) ([]interface{}, []interface{}, []interface{}, error) {
	srcRowsByKey, _, err := e.index(allInSrcCnt, valueAt)
	if err != nil {
		return nil, nil, nil, err
	}
	indexedSrcCnt := len(srcRowsByKey)

	dstDataReader, err := read.New(ctx, db, e.config.FetchSQL, e.config.NewRowFn)
	if err != nil {
		return nil, nil, nil, err
	}

	justInDstByKeyAndId, inSrcAndDstByKey, allInDstCnt, err := e.prepareDataSetsInsDel(ctx, dstDataReader, srcRowsByKey)
	justInSrcByKeyAndId := srcRowsByKey
	if err != nil {
		return nil, nil, nil, fmt.Errorf("merge session exec: failed to fetch target data due to: %w", err)
	}
	e.fillMetricSrcDstComparisonInsDel(allInSrcCnt, indexedSrcCnt, allInDstCnt, justInSrcByKeyAndId, justInDstByKeyAndId, inSrcAndDstByKey)

	dataToInsert, dataToUpdate, dataToDelete := e.categorizeInsDel(justInDstByKeyAndId, justInSrcByKeyAndId)
	e.fillMetricInsUpdDelSetsSummary(dataToInsert, dataToUpdate, dataToDelete)

	return dataToInsert, dataToUpdate, dataToDelete, nil
}

func (e *Executor) prepareDMLDataSetsUpsDel(ctx context.Context, db *sql.DB, valueAt io.ValueAccessor, allInSrcCnt int) ([]interface{}, []interface{}, []interface{}, error) {
	srcRowsByKey, srcRowsIdsByKey, err := e.index(allInSrcCnt, valueAt)
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
		e.metric.FetchAndPrepareSetsTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# FETCHING AND COMPARE TIME: %s\n", e.metric.FetchAndPrepareSetsTime))
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

func (e *Executor) prepareDataSetsInsDel(ctx context.Context, dstDataReader *read.Reader, justInSrcData map[interface{}]interface{}) ([]interface{}, []interface{}, int, error) {
	start := time.Now()
	defer func() {
		e.metric.FetchAndPrepareSetsTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("# FETCHING AND COMPARE TIME: %s\n", e.metric.FetchAndPrepareSetsTime))
	}()

	var justInDstByKey = make([]interface{}, 0)
	var inSrcAndDstByKey = make([]interface{}, 0)

	allInDstCnt := 0

	err := dstDataReader.QueryAll(ctx, func(row interface{}) error {
		key, _, err := e.config.MatchKeyFn(row)
		if err != nil {
			return err
		}

		if _, ok := justInSrcData[key]; ok { // row with match key is present in target table and source data
			delete(justInSrcData, key)
			inSrcAndDstByKey = append(inSrcAndDstByKey, row)
		} else { // row with match key is present in target table but is not present in source data
			justInDstByKey = append(justInDstByKey, row)
		}

		allInDstCnt++
		return nil
	})

	if err != nil {
		return nil, nil, 0, err
	}

	return justInDstByKey, inSrcAndDstByKey, allInDstCnt, nil
}

func (e *Executor) categorize(justInDstByKeyAndId map[interface{}]interface{}, justInSrcByKeyAndId map[interface{}]interface{}, inSrcAndDstByIdButNotByKey []interface{}) ([]interface{}, []interface{}, []interface{}) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING (categorize) INS/UPD/DEL SETS time: %s\n", e.metric.FetchAndPrepareSetsTime))
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

func (e *Executor) categorizeInsDel(justInDstByKeyAndId []interface{}, justInSrcByKeyAndId map[interface{}]interface{}) ([]interface{}, []interface{}, []interface{}) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING INS/UPD/DEL SETS time: %s\n", e.metric.FetchAndPrepareSetsTime))
	}()

	dataToInsert := make([]interface{}, len(justInSrcByKeyAndId))
	dataToUpdate := make([]interface{}, 0)
	dataToDelete := justInDstByKeyAndId

	i := 0
	for _, record := range justInSrcByKeyAndId {
		dataToInsert[i] = record
		i++
	}
	return dataToInsert, dataToUpdate, dataToDelete
}

func (e *Executor) categorizeUpsDel(justInDstByKeyAndId map[interface{}]interface{}, justInSrcByKeyAndId map[interface{}]interface{}, inSrcAndDstByIdButNotByKey []interface{}) ([]interface{}, []interface{}, []interface{}) {
	start := time.Now()
	defer func() {
		e.metric.CategorizeTime = time.Now().Sub(start)
		e.metric.Total.Report = append(e.metric.Total.Report, fmt.Sprintf("CREATING INS/UPD/DEL SETS time: %s\n", e.metric.FetchAndPrepareSetsTime))
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

	for id, _ := range b {
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

func (e *Executor) index(srcCnt int, valueAt io.ValueAccessor) (map[interface{}]interface{}, map[interface{}]interface{}, error) {
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
		srcRowsIdsByKey[key] = identity
	}

	return srcRowsByKey, srcRowsIdsByKey, nil
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
