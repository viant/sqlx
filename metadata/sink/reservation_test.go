package sink

import "testing"

func TestReservationValueContract(t *testing.T) {
	for _, values := range [][]int64{{2, 3, 7}, {-2, -3, -7}, {9, 1, 4}, {0}} {
		r := &Reservation{Sequence: Sequence{Name: "sequence"}, Values: values}
		if err := r.Validate(len(values)); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []*Reservation{nil, {Sequence: Sequence{Name: "sequence"}, Values: []int64{2, 2}}, {Values: []int64{1, 2}}} {
		if err := r.Validate(2); err == nil {
			t.Fatalf("accepted malformed reservation: %+v", r)
		}
	}
}

func TestReservationFromNativeRange(t *testing.T) {
	for _, first := range []int64{7, 17, 27} {
		seq := &Sequence{Name: "range", StartValue: 7, IncrementBy: 5, Value: first + 10}
		r, err := seq.Reservation(2)
		if err != nil || r.Values[0] != first || r.Values[1] != first+5 {
			t.Fatalf("native range: %+v %v", r, err)
		}
	}
	for _, seq := range []*Sequence{nil, {Name: "range", StartValue: 1, Value: 2}, {Name: "range", StartValue: 7, IncrementBy: 5, Value: 7}, {Name: "range", StartValue: 7, IncrementBy: 5, Value: 17, MaxValue: 10}} {
		if _, err := seq.Reservation(2); err == nil {
			t.Fatalf("accepted invalid range: %+v", seq)
		}
	}
}
