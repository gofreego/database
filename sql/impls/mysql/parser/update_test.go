package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

func TestParseUpdateQuery(t *testing.T) {
	type args struct {
		table     *sql.Table
		updates   *sql.Updates
		condition *sql.Condition
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseUpdateQuery(tt.args.table, tt.args.updates, tt.args.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpdateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseUpdateQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseUpdateQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
