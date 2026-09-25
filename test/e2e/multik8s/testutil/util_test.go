package testutil

import (
	"reflect"
	"testing"
)

func TestAddedRBDObjectClones(t *testing.T) {
	tests := []struct {
		name   string
		before map[string][]uint64
		after  map[string][]uint64
		want   map[string][]uint64
	}{
		{
			name:   "nothing changed",
			before: map[string][]uint64{"obj0": {}, "obj1": {37}},
			after:  map[string][]uint64{"obj0": {}, "obj1": {37}},
			want:   map[string][]uint64{},
		},
		{
			name:   "a clone of a trimmed snapshot disappeared",
			before: map[string][]uint64{"obj0": {}, "obj1": {37}},
			after:  map[string][]uint64{"obj0": {}, "obj1": {}},
			want:   map[string][]uint64{},
		},
		{
			name:   "an object was written after the latest snapshot",
			before: map[string][]uint64{"obj0": {}, "obj1": {37}},
			after:  map[string][]uint64{"obj0": {42}, "obj1": {37, 42}},
			want:   map[string][]uint64{"obj0": {42}, "obj1": {42}},
		},
		{
			name:   "a new object appeared with a clone",
			before: map[string][]uint64{"obj0": {}},
			after:  map[string][]uint64{"obj0": {}, "obj1": {42}},
			want:   map[string][]uint64{"obj1": {42}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AddedRBDObjectClones(tt.before, tt.after)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddedRBDObjectClones() = %v, want %v", got, tt.want)
			}
		})
	}
}
