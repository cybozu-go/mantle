package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrittenRBDObjects(t *testing.T) {
	tests := []struct {
		name   string
		before map[string][]string
		after  map[string][]string
		want   []string
	}{
		{
			name:   "nothing changed",
			before: map[string][]string{"obj0": {"head"}, "obj1": {"37", "head"}},
			after:  map[string][]string{"obj0": {"head"}, "obj1": {"37", "head"}},
			want:   nil,
		},
		{
			name:   "a clone disappeared",
			before: map[string][]string{"obj0": {"head"}, "obj1": {"37", "head"}},
			after:  map[string][]string{"obj0": {"head"}, "obj1": {"head"}},
			want:   nil,
		},
		{
			name:   "an object disappeared",
			before: map[string][]string{"obj0": {"head"}, "obj1": {"37"}},
			after:  map[string][]string{"obj0": {"head"}},
			want:   nil,
		},
		{
			name:   "a clone was created",
			before: map[string][]string{"obj0": {"head"}, "obj1": {"37", "head"}},
			after:  map[string][]string{"obj0": {"42", "head"}, "obj1": {"37", "head"}},
			want:   []string{"obj0"},
		},
		{
			name:   "a clone was created while others disappeared",
			before: map[string][]string{"obj0": {"30", "37", "head"}},
			after:  map[string][]string{"obj0": {"42", "head"}},
			want:   []string{"obj0"},
		},
		{
			name:   "an object was created",
			before: map[string][]string{"obj0": {"head"}},
			after:  map[string][]string{"obj0": {"head"}, "obj1": {"head"}},
			want:   []string{"obj1"},
		},
		{
			name:   "the head of an object was created again",
			before: map[string][]string{"obj0": {"37"}},
			after:  map[string][]string{"obj0": {"37", "head"}},
			want:   []string{"obj0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, WrittenRBDObjects(tt.before, tt.after))
		})
	}
}
