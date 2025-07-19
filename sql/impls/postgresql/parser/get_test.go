package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

type mockRecords struct {
	table   *sql.Table
	columns []*sql.Field
}

func (m *mockRecords) Table() *sql.Table        { return m.table }
func (m *mockRecords) Columns() []*sql.Field    { return m.columns }
func (m *mockRecords) Scan(rows sql.Rows) error { return nil }

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
			name: "valid record",
			args: args{
				record: &records.User{Id: 1},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users WHERE id = $1",
			wantErr: false,
		},
		{
			name: "record with nil table",
			args: args{
				record: &mockNoTableRecord{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseGetByIDQuery(tt.args.record)
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
		want1   []int
		wantErr bool
	}{
		{
			name: "nil filter",
			args: args{
				filter:  nil,
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "simple indexed filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "name",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.EQ,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users WHERE name = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "complex filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Operator: sql.AND,
						Conditions: []sql.Condition{
							{
								Field:    "email",
								Value:    sql.NewIndexedValue(0),
								Operator: sql.EQ,
							},
							{
								Field:    "is_active",
								Value:    sql.NewValue(1),
								Operator: sql.EQ,
							},
							{
								Field:    "name",
								Value:    sql.NewIndexedValue(2),
								Operator: sql.LIKE,
							},
						},
					},
					GroupBy: sql.NewGroupBy("is_active"),
					Sort:    sql.NewSort().Add("created_at", sql.Desc),
					Limit:   sql.NewValue(int64(10)),
					Offset:  sql.NewIndexedValue(1),
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, is_active, created_at, updated_at FROM users WHERE (email = $1 AND is_active = 1 AND name LIKE $2) GROUP BY (is_active) ORDER BY created_at DESC LIMIT 10 OFFSET $3",
			want1:   []int{0, 2, 1},
			wantErr: false,
		},
		{
			name: "records with nil table",
			args: args{
				filter:  nil,
				records: &mockRecords{table: nil, columns: []*sql.Field{sql.NewField("id")}},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseGetByFilterQuery(tt.args.filter, tt.args.records)
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
