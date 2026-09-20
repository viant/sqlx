package sink

import (
	"fmt"
	"math"
)

// Reservation contains actual values owned by the native allocator. Values need
// not be consecutive, increasing, or reclaimable after transaction rollback.
// Sequence identifies their authority; it is not a fabricated batch range.
type Reservation struct {
	Sequence Sequence
	Values   []int64
}

func (r *Reservation) Validate(count int) error {
	if r == nil || count <= 0 || len(r.Values) != count || r.Sequence.Name == "" {
		return fmt.Errorf("native reservation requires an identity and exactly %d values", count)
	}
	seen := make(map[int64]bool, count)
	for _, value := range r.Values {
		if seen[value] {
			return fmt.Errorf("native reservation returned duplicate value %d", value)
		}
		seen[value] = true
	}
	return nil
}

// Reservation converts only a genuinely reserved native range. It is
// used by range allocators, never to infer values returned by nextval calls.
func (sequence *Sequence) Reservation(count int) (*Reservation, error) {
	if sequence == nil || count <= 0 || sequence.IncrementBy <= 0 || int64(count) > math.MaxInt64/sequence.IncrementBy {
		return nil, fmt.Errorf("invalid native reservation range")
	}
	width := int64(count) * sequence.IncrementBy
	if sequence.StartValue < 0 && sequence.Value > math.MaxInt64+sequence.StartValue || sequence.Value < math.MinInt64+width {
		return nil, fmt.Errorf("native reservation range overflows")
	}
	if sequence.Value > sequence.StartValue {
		aligned := sequence.Value - (sequence.Value-sequence.StartValue)%sequence.IncrementBy
		if aligned < math.MinInt64+width {
			return nil, fmt.Errorf("native aligned reservation underflows")
		}
	}
	first := sequence.MinValue(int64(count))
	if first < sequence.StartValue || first > math.MaxInt64-width || first+width > sequence.Value {
		return nil, fmt.Errorf("invalid native reservation bounds")
	}
	r := &Reservation{Sequence: *sequence, Values: make([]int64, count)}
	for i := range r.Values {
		r.Values[i] = first + int64(i)*sequence.IncrementBy
	}
	if sequence.MaxValue > 0 && r.Values[count-1] > sequence.MaxValue {
		return nil, fmt.Errorf("native reservation exceeds sequence maximum")
	}
	return r, r.Validate(count)
}
