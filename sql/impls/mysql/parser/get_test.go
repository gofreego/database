package parser

import (
	"reflect"
	"testing"
	"time"

	"github.com/gofreego/database/sql"
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
			want: "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE id = ?",
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
			name: "with nil filter",
			args: args{
				filter:  nil,
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users",
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
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "with LIKE filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "name",
						Value:    sql.NewValue("john%"),
						Operator: sql.LIKE,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE name LIKE 'john%'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with IN filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "status",
						Value:    sql.NewValue([]any{"active", "pending"}),
						Operator: sql.IN,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE status IN ('active', 'pending')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with IS NULL filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "deleted_at",
						Operator: sql.ISNULL,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE deleted_at IS NULL",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with BETWEEN filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "age",
						Value:    sql.NewValue([]any{18, 65}),
						Operator: sql.BETWEEN,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE age BETWEEN 18 AND 65",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with time filter",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "created_at",
						Value:    sql.NewValue(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
						Operator: sql.GT,
					},
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE created_at > '2023-01-01T00:00:00Z'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "with group by, order by, limit, offset",
			args: args{
				filter: &sql.Filter{
					GroupBy: sql.NewGroupBy("city", "country"),
					Sort:    sql.NewSort().Add("age", sql.Asc),
					Limit:   sql.NewValue(int64(10)),
					Offset:  sql.NewIndexedValue(1),
				},
				records: &records.Users{},
			},
			want:    "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE 1 GROUP BY (city, country) ORDER BY age ASC LIMIT 10 OFFSET ?",
			want1:   []int{1},
			wantErr: false,
		},
		{
			name: "with invalid limit value",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue("not-an-int"),
				},
				records: &records.Users{},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "with invalid offset value",
			args: args{
				filter: &sql.Filter{
					Offset: sql.NewValue(-1),
				},
				records: &records.Users{},
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
