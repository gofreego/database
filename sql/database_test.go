package sql

import (
	"reflect"
	"testing"
)

func TestGetValues(t *testing.T) {
	type args struct {
		indexs []int
		values []any
	}
	tests := []struct {
		name string
		args args
		want []any
	}{
		{
			name: "basic values",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{1, 2, 3},
			},
			want: []any{1, 2, 3},
		},
		{
			name: "empty indexs",
			args: args{
				indexs: []int{},
				values: []any{1, 2, 3},
			},
			want: nil,
		},
		{
			name: "single value",
			args: args{
				indexs: []int{0},
				values: []any{"test"},
			},
			want: []any{"test"},
		},
		{
			name: "mixed types",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{"string", 42, true},
			},
			want: []any{"string", 42, true},
		},
		{
			name: "with array values",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{[]int{1, 2, 3}, "string", []string{"a", "b"}},
			},
			want: []any{1, 2, 3, "string", "a", "b"},
		},
		{
			name: "nested arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]int{1, 2}, []int{3, 4}},
			},
			want: []any{1, 2, 3, 4},
		},
		{
			name: "string arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]string{"hello", "world"}, []string{"test"}},
			},
			want: []any{"hello", "world", "test"},
		},
		{
			name: "mixed arrays and scalars",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{[]int{1, 2}, "middle", []string{"end"}},
			},
			want: []any{1, 2, "middle", "end"},
		},
		{
			name: "empty arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]int{}, []string{}},
			},
			want: []any{},
		},
		{
			name: "complex nested structure",
			args: args{
				indexs: []int{0, 1, 2, 3},
				values: []any{
					[]int{1, 2},
					"string",
					[]string{"a", "b", "c"},
					[]bool{true},
				},
			},
			want: []any{1, 2, "string", "a", "b", "c", true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetValues(tt.args.indexs, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValues() = %v, want %v", got, tt.want)
			}
		})
	}
}
