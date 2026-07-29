package aerospike

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
)

type Source struct {
	cache         *Cache
	entry         *cache.Entry
	scanner       cache.ScannerFn
	columnsHolder *cache.ColumnsHolder
	xtypesHolder  *cache.XTypesHolder
}

func (s *Source) Err() error {
	return nil
}

func (s *Source) ConvertColumns() ([]io.Column, error) {
	s.ensureColumnsHolder()
	return s.columnsHolder.ConvertColumns()
}

func (s *Source) Scanner(ctx context.Context) cache.ScannerFn {
	if s.scanner != nil {
		return s.scanner
	}

	if s.entry.Meta.Projected() {
		s.scanner = cache.NewProjectedScanner(s.entry, s.entry.Meta.ProjectedIndexes, s.cache.typeHolder, s.cache.recorder)
		return s.scanner
	}

	s.scanner = cache.NewScanner(s.cache.typeHolder, s.cache.recorder).New(s.entry)
	return s.scanner
}

func (s *Source) XTypes() []*xunsafe.Type {
	s.ensureXTypesHolder()

	return s.xtypesHolder.XTypes()
}

func (s *Source) CheckType(ctx context.Context, values []interface{}) (bool, error) {
	ok, err := s.cache.UpdateType(ctx, s.entry, values)
	if !ok || err != nil {
		mismatch := typeMismatch(s.cache, s.entry)
		s.cache.logWarmupf("aerospike cache check_type_failure set=%s projected=%t projected_indexes=%v fields=%s stored_fields=%s meta_type=%v effective_type=%v dest_type=%v mismatch_index=%d mismatch_field=%q mismatch_stored_field=%q mismatch_dest_type=%q mismatch_dest_type_normalized=%q mismatch_cached_type=%q mismatch_cached_type_normalized=%q mismatch_reason=%q err=%v\n",
			s.cache.set,
			s.entry != nil && s.entry.Meta.Projected(),
			projectedIndexes(s.entry),
			fieldNames(s.entry),
			storedFieldNames(s.entry),
			metaTypes(s.entry),
			effectiveTypes(s.entry),
			destinationTypes(values),
			mismatchIndex(mismatch),
			mismatchFieldName(s.entry, mismatch),
			mismatchStoredFieldName(s.entry, mismatch),
			mismatchDestinationType(mismatch),
			mismatchNormalizedDestinationType(mismatch),
			mismatchCachedType(mismatch),
			mismatchNormalizedCachedType(mismatch),
			mismatchReason(mismatch),
			err,
		)
	}
	return ok, err
}

func (s *Source) Close(ctx context.Context) error {
	return s.cache.Close(ctx, s.entry)
}

func (s *Source) Next() bool {
	return s.entry.Next()
}

func (s *Source) Rollback(ctx context.Context) error {
	return s.cache.Delete(ctx, s.entry)
}

func (s *Source) ensureColumnsHolder() {
	if s.columnsHolder != nil {
		return
	}

	s.columnsHolder = cache.NewColumnsHolder(s.entry)
}

func (s *Source) ensureXTypesHolder() {
	if s.xtypesHolder != nil {
		return
	}

	s.xtypesHolder = cache.NewXTypeHolder(s.entry)
}

func projectedIndexes(entry *cache.Entry) []int {
	if entry == nil {
		return nil
	}
	return entry.Meta.ProjectedIndexes
}

func fieldNames(entry *cache.Entry) []string {
	if entry == nil {
		return nil
	}
	result := make([]string, 0, len(entry.Meta.Fields))
	for _, field := range entry.Meta.Fields {
		if field == nil {
			result = append(result, "<nil>")
			continue
		}
		result = append(result, field.Name())
	}
	return result
}

func storedFieldNames(entry *cache.Entry) []string {
	if entry == nil {
		return nil
	}
	result := make([]string, 0, len(entry.Meta.StoredFields))
	for _, field := range entry.Meta.StoredFields {
		result = append(result, field.Name)
	}
	return result
}

func metaTypes(entry *cache.Entry) []string {
	if entry == nil {
		return nil
	}
	return entry.Meta.Type
}

func effectiveTypes(entry *cache.Entry) []string {
	if entry == nil {
		return nil
	}
	return entry.Meta.EffectiveType()
}

func typeMismatch(c *Cache, entry *cache.Entry) *cache.TypeMismatch {
	if c == nil || c.typeHolder == nil || entry == nil {
		return nil
	}
	return c.typeHolder.Mismatch(entry)
}

func mismatchIndex(mismatch *cache.TypeMismatch) int {
	if mismatch == nil {
		return -1
	}
	return mismatch.Index
}

func mismatchFieldName(entry *cache.Entry, mismatch *cache.TypeMismatch) string {
	if entry == nil || mismatch == nil || mismatch.Index < 0 || mismatch.Index >= len(entry.Meta.Fields) {
		return ""
	}
	field := entry.Meta.Fields[mismatch.Index]
	if field == nil {
		return ""
	}
	return field.Name()
}

func mismatchStoredFieldName(entry *cache.Entry, mismatch *cache.TypeMismatch) string {
	if entry == nil || mismatch == nil || mismatch.Index < 0 {
		return ""
	}
	if mismatch.Index < len(entry.Meta.StoredFields) {
		return entry.Meta.StoredFields[mismatch.Index].Name
	}
	return ""
}

func mismatchDestinationType(mismatch *cache.TypeMismatch) string {
	if mismatch == nil {
		return ""
	}
	return mismatch.DestinationType
}

func mismatchNormalizedDestinationType(mismatch *cache.TypeMismatch) string {
	if mismatch == nil {
		return ""
	}
	return mismatch.NormalizedDestType
}

func mismatchCachedType(mismatch *cache.TypeMismatch) string {
	if mismatch == nil {
		return ""
	}
	return mismatch.CachedType
}

func mismatchNormalizedCachedType(mismatch *cache.TypeMismatch) string {
	if mismatch == nil {
		return ""
	}
	return mismatch.NormalizedCachedType
}

func mismatchReason(mismatch *cache.TypeMismatch) string {
	if mismatch == nil {
		return ""
	}
	return mismatch.Reason
}

func destinationTypes(values []interface{}) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil {
			result = append(result, "<nil>")
			continue
		}
		result = append(result, fmt.Sprintf("%T", value))
	}
	return result
}
