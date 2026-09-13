package io

import (
	"reflect"
	"testing"
)

type pathEmbedded struct {
	ID int `sqlx:"id|identity"`
}
type pathNested struct {
	ID int `sqlx:"id"`
}
type pathRecord struct {
	*pathEmbedded
	Left    pathNested  `sqlx:"ns=l_"`
	Right   *pathNested `sqlx:"ns=r_"`
	Ignored int         `sqlx:"-"`
}

func TestMatcherFieldIndexRetainsNativeOwnerChain(t *testing.T) {
	for _, tc := range []struct {
		column string
		want   []int
	}{
		{"identity", []int{0, 0}},
		{"l_id", []int{1, 0}},
		{"r_id", []int{2, 0}},
		{"ignored", nil},
	} {
		t.Run(tc.column, func(t *testing.T) {
			matched, err := NewMatcher(nil).Match(reflect.TypeOf(pathRecord{}), NamesToColumns([]string{tc.column}))
			if tc.want == nil {
				if !IsMatchedError(err) || len(matched[0].FieldIndex()) != 0 {
					t.Fatalf("unmapped=%+v err=%v", matched, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			path := matched[0].FieldIndex()
			if !reflect.DeepEqual(path, tc.want) {
				t.Fatalf("path=%v want=%v", path, tc.want)
			}
			path[0] = 999
			if !reflect.DeepEqual(matched[0].FieldIndex(), tc.want) {
				t.Fatal("path alias escaped")
			}
		})
	}
}
