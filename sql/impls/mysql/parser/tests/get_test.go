package tests

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseGetByIDQuery(t *testing.T) {
	type args struct {
		record sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				record: &records.User{
					Id: 1,
				},
			},
			want: "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users WHERE id = ?",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parser.ParseGetByIDQuery(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseGetByIDQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseGetByIDQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseGetByFilterQuery(t *testing.T) {
	type args struct {
		filter  *sql.Filter
		records sql.Records
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []*sql.Value
		wantErr bool
	}{
		{
			name: "with nil filter",
			args: args{
				filter:  nil,
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with empty filter condition",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "id",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.EQ,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users WHERE id = ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0)},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parser.ParseGetByFilterQuery(tt.args.filter, tt.args.records)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseGetByFilterQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseGetByFilterQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseGetByFilterQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
