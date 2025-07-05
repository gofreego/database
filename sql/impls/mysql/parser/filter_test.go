package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

func Test_parseFilter(t *testing.T) {
	type args struct {
		filter *sql.Filter
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []*sql.Value
		wantErr bool
	}{
		{
			name: "test with nil filter",
			args: args{
				filter: nil,
			},
			want:    "",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with simple filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:      "name",
						ValueIndex: 0,
						Operator:   sql.EQ,
					},
				},
			},
			want:    "WHERE name = ?",
			want1:   []*sql.Value{sql.AnyValue(0)},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseFilter(tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseFilter() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("parseFilter() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
