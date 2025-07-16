package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

type mockRecords struct {
	table   *sql.Table
	columns []string
}

func (m *mockRecords) Table() *sql.Table        { return m.table }
func (m *mockRecords) Columns() []string        { return m.columns }
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
				record: &mockIdOnlyRecord{Id: 1},
			},
			want:    "SELECT id FROM mock WHERE id = ?",
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
			got, err := ParseGetByIDQuery(tt.args.record)
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
				records: &mockRecords{table: sql.NewTable("users"), columns: []string{"id", "name"}},
			},
			want:    "SELECT id, name FROM users",
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
				records: &mockRecords{table: sql.NewTable("users"), columns: []string{"id", "name"}},
			},
			want:    "SELECT id, name FROM users WHERE name = @p1",
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
								Field:    "age",
								Value:    sql.NewValue(30),
								Operator: sql.GT,
							},
							{
								Field:    "name",
								Value:    sql.NewIndexedValue(2),
								Operator: sql.LIKE,
							},
						},
					},
					GroupBy: sql.NewGroupBy("city", "country"),
					Sort:    sql.NewSort().Add("age", sql.Asc),
					Limit:   sql.NewValue(int64(10)),
					Offset:  sql.NewIndexedValue(1),
				},
				records: &mockRecords{table: sql.NewTable("users"), columns: []string{"id", "name"}},
			},
			want:    "SELECT id, name FROM users WHERE (email = @p1 AND age > 30 AND name LIKE @p2) GROUP BY (city, country) ORDER BY age ASC LIMIT 10 OFFSET @p3",
			want1:   []int{0, 2, 1},
			wantErr: false,
		},
		{
			name: "records with nil table",
			args: args{
				filter:  nil,
				records: &mockRecords{table: nil, columns: []string{"id"}},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseGetByFilterQuery(tt.args.filter, tt.args.records)
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
