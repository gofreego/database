package parser

import (
	"reflect"
	"testing"
	"time"

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
						Field:    "name",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.EQ,
					},
				},
			},
			want:    "WHERE name = ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0)},
			wantErr: false,
		},
		{
			name: "test with complex filter",
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
			},
			want:    "WHERE (email = ? AND age > 30 AND name LIKE ?) GROUP BY (city, country) ORDER BY age ASC LIMIT 10 OFFSET ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0), sql.NewIndexedValue(2).WithType(sql.String), sql.NewIndexedValue(1).WithType(sql.Int)},
			wantErr: false,
		},
		{
			name: "test with LIKE fixed value",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "name",
						Value:    sql.NewValue("john%"),
						Operator: sql.LIKE,
					},
				},
			},
			want:    "WHERE name LIKE 'john%'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with IN fixed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "status",
						Value:    sql.NewValue([]any{"active", "pending"}),
						Operator: sql.IN,
					},
				},
			},
			want:    "WHERE status IN ('active', 'pending')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with IS NULL",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "deleted_at",
						Operator: sql.ISNULL,
					},
				},
			},
			want:    "WHERE deleted_at IS NULL",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with BETWEEN fixed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "age",
						Value:    sql.NewValue([]any{18, 65}),
						Operator: sql.BETWEEN,
					},
				},
			},
			want:    "WHERE age BETWEEN 18 AND 65",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with time value",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "created_at",
						Value:    sql.NewValue(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
						Operator: sql.GT,
					},
				},
			},
			want:    "WHERE created_at > '2023-01-01T00:00:00Z'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with invalid limit value",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue("not-an-int"),
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with invalid offset value",
			args: args{
				filter: &sql.Filter{
					Offset: sql.NewValue(-1),
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
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
