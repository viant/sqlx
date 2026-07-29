package cache

type Meta struct {
	SQL          string
	Args         []byte
	Type         []string
	Signature    string
	ExpiryTimeMs int
	Fields       []*Field
	StoredFields []ProjectionField
	// ProjectedIndexes maps requested result columns to stored cached row ordinals.
	// It is runtime-only metadata used when reading a warmup superset as a subset.
	ProjectedIndexes []int `json:"-" yaml:"-"`

	URL string `json:"-" yaml:"-"`
}

func (m *Meta) Projected() bool {
	return len(m.ProjectedIndexes) > 0
}

func (m *Meta) EffectiveFields() []*Field {
	if !m.Projected() {
		return m.Fields
	}
	result := make([]*Field, 0, len(m.ProjectedIndexes))
	for _, index := range m.ProjectedIndexes {
		if index < 0 || index >= len(m.Fields) {
			continue
		}
		result = append(result, m.Fields[index])
	}
	return result
}

func (m *Meta) EffectiveType() []string {
	if m.Projected() {
		if len(m.Type) == len(m.Fields) {
			result := make([]string, 0, len(m.ProjectedIndexes))
			for _, index := range m.ProjectedIndexes {
				if index < 0 || index >= len(m.Type) {
					continue
				}
				result = append(result, m.Type[index])
			}
			return result
		}
		if len(m.Type) == 0 {
			fields := m.EffectiveFields()
			result := make([]string, 0, len(fields))
			for _, field := range fields {
				if field == nil || field.ScanType() == nil {
					result = append(result, "")
					continue
				}
				result = append(result, field.ScanType().String())
			}
			return result
		}
	}
	if len(m.Type) == 0 {
		return nil
	}
	if !m.Projected() || len(m.Type) != len(m.Fields) {
		return m.Type
	}
	result := make([]string, len(m.Type))
	copy(result, m.Type)
	return result
}
