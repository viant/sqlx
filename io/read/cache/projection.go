package cache

import (
	"fmt"
	"strings"
)

// Projection owns native warmup projection matching for every cache provider.
type Projection struct{ Stored []ProjectionField }

func (p Projection) Indexes(requested []ProjectionField) ([]int, bool, string, error) {
	return warmupProjectionIndexes(p.Stored, requested)
}
func (p Projection) Mismatch(requested []ProjectionField) (int, string, string) {
	return exactProjectionMismatch(p.Stored, requested)
}

func (m *Meta) ApplyProjection(requested []ProjectionField) bool {
	if len(requested) == 0 {
		return true
	}
	stored := m.StoredFields
	if len(stored) == 0 {
		stored = make([]ProjectionField, len(m.Fields))
		for i, field := range m.Fields {
			stored[i] = ProjectionField{ColumnName: field.Name()}
		}
	}
	indexes, ok, _, err := (Projection{Stored: stored}).Indexes(requested)
	if err != nil || !ok {
		return false
	}
	m.ProjectedIndexes = indexes
	return true
}

func warmupProjectionIndexes(storedFields []ProjectionField, requestedFields []ProjectionField) ([]int, bool, string, error) {
	if len(requestedFields) == 0 {
		return nil, false, "requested_projection_empty", nil
	}
	if len(storedFields) == 0 {
		return nil, false, "stored_projection_empty", nil
	}
	if hasGroupedProjectionFields(storedFields) || hasGroupedProjectionFields(requestedFields) {
		return groupedWarmupProjectionIndexes(storedFields, requestedFields)
	}
	if indexes, ok := exactProjectionIndexes(storedFields, requestedFields); ok {
		return indexes, true, "", nil
	}
	return nonGroupedWarmupProjectionIndexes(storedFields, requestedFields)
}

func hasGroupedProjectionFields(fields []ProjectionField) bool {
	for _, field := range fields {
		if field.DimensionKey != "" || field.MeasureKey != "" {
			return true
		}
	}
	return false
}

func nonGroupedWarmupProjectionIndexes(storedFields []ProjectionField, requestedFields []ProjectionField) ([]int, bool, string, error) {
	const ambiguousProjectionIndex = -2
	storedByName := map[string]int{}
	storedByWeak := map[string]int{}
	for index, field := range storedFields {
		for _, name := range projectionFieldStrongLookup(field) {
			if existing, exists := storedByName[name]; !exists {
				storedByName[name] = index
			} else if existing != index {
				storedByName[name] = ambiguousProjectionIndex
			}
		}
		for _, name := range projectionFieldWeakLookup(field) {
			if existing, exists := storedByWeak[name]; !exists {
				storedByWeak[name] = index
			} else if existing != index {
				storedByWeak[name] = ambiguousProjectionIndex
			}
		}
	}
	indexes := make([]int, 0, len(requestedFields))
	for _, field := range requestedFields {
		index, ok, reason := resolveProjectionFieldIndex(field, storedByName, ambiguousProjectionIndex, projectionFieldStrongLookup)
		if !ok && reason != "" {
			return nil, false, reason, nil
		}
		if index == -1 {
			index, ok, reason = resolveProjectionFieldIndex(field, storedByWeak, ambiguousProjectionIndex, projectionFieldWeakLookup)
			if !ok && reason != "" {
				return nil, false, reason, nil
			}
		}
		if index == -1 {
			return nil, false, "non_grouped_missing_field", nil
		}
		indexes = append(indexes, index)
	}
	return indexes, true, "", nil
}

func resolveProjectionFieldIndex(field ProjectionField, stored map[string]int, ambiguousProjectionIndex int, lookup func(ProjectionField) []string) (int, bool, string) {
	index := -1
	for _, name := range lookup(field) {
		candidate, ok := stored[name]
		if !ok {
			continue
		}
		if candidate == ambiguousProjectionIndex {
			return -1, false, "non_grouped_ambiguous_stored_alias"
		}
		if index == -1 {
			index = candidate
			continue
		}
		if index != candidate {
			return -1, false, "non_grouped_requested_alias_conflict"
		}
	}
	return index, true, ""
}

func exactProjectionIndexes(storedFields []ProjectionField, requestedFields []ProjectionField) ([]int, bool) {
	index, _, _ := exactProjectionMismatch(storedFields, requestedFields)
	if index != -1 {
		return nil, false
	}
	indexes := make([]int, len(requestedFields))
	for i := range requestedFields {
		indexes[i] = i
	}
	return indexes, true
}

func exactProjectionMismatch(storedFields []ProjectionField, requestedFields []ProjectionField) (int, string, string) {
	if len(storedFields) != len(requestedFields) {
		return minInt(len(storedFields), len(requestedFields)), "", ""
	}
	for i := range requestedFields {
		if !projectionFieldStrongIdentityOverlap(storedFields[i], requestedFields[i]) {
			return i, projectionFieldSummary(storedFields[i]), projectionFieldSummary(requestedFields[i])
		}
	}
	return -1, "", ""
}

func groupedWarmupProjectionIndexes(storedFields []ProjectionField, requestedFields []ProjectionField) ([]int, bool, string, error) {
	storedDimensions, storedMeasures, reason, ok := groupedProjectionPartitions(storedFields)
	if !ok {
		return nil, false, reason, nil
	}
	requestedDimensions, _, reason, ok := groupedProjectionPartitions(requestedFields)
	if !ok {
		return nil, false, reason, nil
	}
	if len(storedDimensions) != len(requestedDimensions) {
		return nil, false, "grouped_dimension_mismatch", nil
	}

	storedDimensionSet := map[string]bool{}
	for _, field := range storedDimensions {
		if field.DimensionKey == "" || storedDimensionSet[field.DimensionKey] {
			return nil, false, "grouped_duplicate_dimension", nil
		}
		storedDimensionSet[field.DimensionKey] = true
	}
	requestedDimensionSet := map[string]bool{}
	for _, field := range requestedDimensions {
		if field.DimensionKey == "" || requestedDimensionSet[field.DimensionKey] {
			return nil, false, "grouped_duplicate_dimension", nil
		}
		requestedDimensionSet[field.DimensionKey] = true
		if !storedDimensionSet[field.DimensionKey] {
			return nil, false, "grouped_dimension_mismatch", nil
		}
	}

	storedMeasureIndexes := map[string]int{}
	for _, field := range storedMeasures {
		if _, exists := storedMeasureIndexes[field.MeasureKey]; exists {
			return nil, false, "grouped_duplicate_measure", nil
		}
		storedMeasureIndexes[field.MeasureKey] = indexOfProjectionField(storedFields, field)
	}

	indexes := make([]int, 0, len(requestedFields))
	for _, requested := range requestedFields {
		if requested.DimensionKey != "" {
			index := indexOfProjectionFieldByDimensionKey(storedFields, requested.DimensionKey)
			if index == -1 {
				return nil, false, "grouped_dimension_mismatch", nil
			}
			indexes = append(indexes, index)
			continue
		}
		if requested.MeasureKey == "" {
			return nil, false, "grouped_invalid_metadata", nil
		}
		index, ok := storedMeasureIndexes[requested.MeasureKey]
		if !ok {
			return nil, false, "grouped_missing_measure", nil
		}
		indexes = append(indexes, index)
	}

	return indexes, true, "", nil
}

func groupedProjectionPartitions(fields []ProjectionField) ([]ProjectionField, []ProjectionField, string, bool) {
	dimensions := make([]ProjectionField, 0)
	measures := make([]ProjectionField, 0)
	for _, field := range fields {
		switch {
		case field.DimensionKey != "" && field.MeasureKey == "":
			dimensions = append(dimensions, field)
		case field.MeasureKey != "" && field.DimensionKey == "":
			measures = append(measures, field)
		default:
			return nil, nil, "grouped_invalid_metadata", false
		}
	}
	return dimensions, measures, "", true
}

func indexOfProjectionField(fields []ProjectionField, target ProjectionField) int {
	for i, field := range fields {
		if field.DimensionKey == target.DimensionKey && field.MeasureKey == target.MeasureKey {
			return i
		}
	}
	return -1
}

func indexOfProjectionFieldByDimensionKey(fields []ProjectionField, dimensionKey string) int {
	for i, field := range fields {
		if field.DimensionKey == dimensionKey {
			return i
		}
	}
	return -1
}

func projectionFieldStrongLookup(field ProjectionField) []string {
	var result []string
	add := func(value string) {
		value = normalizeProjectionFieldName(value)
		if value == "" {
			return
		}
		result = append(result, value)
	}
	add(field.DimensionKey)
	add(field.MeasureKey)
	add(field.Name)
	add(field.FieldName)
	add(field.ColumnName)
	return result
}

func projectionFieldWeakLookup(field ProjectionField) []string {
	var result []string
	add := func(value string) {
		value = normalizeProjectionFieldName(value)
		if value == "" {
			return
		}
		result = append(result, value)
	}
	add(field.Source)
	return result
}

func projectionFieldStrongIdentityOverlap(stored ProjectionField, requested ProjectionField) bool {
	storedValues := projectionFieldStrongLookup(stored)
	requestedValues := projectionFieldStrongLookup(requested)
	for _, storedValue := range storedValues {
		for _, requestedValue := range requestedValues {
			if storedValue == requestedValue {
				return true
			}
		}
	}
	return false
}

func projectionFieldSummary(field ProjectionField) string {
	return fmt.Sprintf("name=%q field=%q column=%q dim=%q measure=%q lookup=%v source=%q",
		field.Name,
		field.FieldName,
		field.ColumnName,
		field.DimensionKey,
		field.MeasureKey,
		field.Lookup,
		field.Source,
	)
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func projectionFieldLookup(field ProjectionField) []string {
	result := projectionFieldStrongLookup(field)
	result = append(result, projectionFieldWeakLookup(field)...)
	return result
}

func normalizeProjectionFieldName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if index := strings.LastIndex(value, "."); index != -1 && index+1 < len(value) {
		value = value[index+1:]
	}
	value = strings.Trim(value, "`\"")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, ".", "")
	return value
}
