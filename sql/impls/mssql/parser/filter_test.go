package parser

import (
	"reflect"
	"testing"
	"time"

	"github.com/gofreego/database/sql"
)

// Tests for mssql filter parser
func Test_parseFilter(t *testing.T) {
	type args struct {
		filter    *sql.Filter
		lastIndex *int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []int
		wantErr bool
	}{
		{
			name: "test with nil filter",
			args: args{
				filter:    nil,
				lastIndex: new(int),
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
				lastIndex: new(int),
			},
			want:    "WHERE name = @p1",
			want1:   []int{0},
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
				lastIndex: new(int),
			},
			want:    "WHERE (email = @p1 AND age > 30 AND name LIKE @p2) GROUP BY (city, country) ORDER BY age ASC OFFSET @p3 ROWS FETCH NEXT 10 ROWS ONLY",
			want1:   []int{0, 2, 1},
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
				lastIndex: new(int),
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
				lastIndex: new(int),
			},
			want:    "WHERE status IN ('active', 'pending')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with IN indexed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "status",
						Value:    sql.NewIndexedValue(0).WithCount(3),
						Operator: sql.IN,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE status IN (@p1, @p2, @p3)",
			want1:   []int{0},
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
				lastIndex: new(int),
			},
			want:    "WHERE deleted_at IS NULL",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with IS NOT NULL",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "email",
						Operator: sql.ISNOTNULL,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE email IS NOT NULL",
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
				lastIndex: new(int),
			},
			want:    "WHERE age BETWEEN 18 AND 65",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with BETWEEN indexed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "age",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.BETWEEN,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE (age BETWEEN @p1 AND @p2)",
			want1:   []int{0},
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
				lastIndex: new(int),
			},
			want:    "WHERE created_at > '2023-01-01T00:00:00Z'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with OR condition",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Operator: sql.OR,
						Conditions: []sql.Condition{
							{
								Field:    "status",
								Value:    sql.NewValue("active"),
								Operator: sql.EQ,
							},
							{
								Field:    "status",
								Value:    sql.NewValue("pending"),
								Operator: sql.EQ,
							},
						},
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE (status = 'active' OR status = 'pending')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with NOT condition",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Operator: sql.NOT,
						Conditions: []sql.Condition{
							{
								Field:    "status",
								Value:    sql.NewValue("deleted"),
								Operator: sql.EQ,
							},
						},
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE NOT (status = 'deleted')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with column comparison",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "updated_at",
						Value:    sql.NewColumnValue("created_at"),
						Operator: sql.GTE,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE updated_at >= created_at",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with multiple sort fields",
			args: args{
				filter: &sql.Filter{
					Sort: sql.NewSort().
						Add("created_at", sql.Desc).
						Add("name", sql.Asc),
				},
				lastIndex: new(int),
			},
			want:    "WHERE 1=1 ORDER BY created_at DESC, name ASC",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with limit only",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue(int64(5)),
				},
				lastIndex: new(int),
			},
			want:    "WHERE 1=1 ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with offset only",
			args: args{
				filter: &sql.Filter{
					Offset: sql.NewValue(int64(10)),
				},
				lastIndex: new(int),
			},
			want:    "WHERE 1=1 ORDER BY (SELECT NULL) OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with indexed limit",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewIndexedValue(0),
				},
				lastIndex: new(int),
			},
			want:    "WHERE 1=1 ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT @p1 ROWS ONLY",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with indexed offset",
			args: args{
				filter: &sql.Filter{
					Offset: sql.NewIndexedValue(1),
				},
				lastIndex: new(int),
			},
			want:    "WHERE 1=1 ORDER BY (SELECT NULL) OFFSET @p1 ROWS FETCH NEXT 10 ROWS ONLY",
			want1:   []int{1},
			wantErr: false,
		},
		{
			name: "test with NOT IN fixed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "status",
						Value:    sql.NewValue([]any{"deleted", "archived"}),
						Operator: sql.NOTIN,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE status NOT IN ('deleted', 'archived')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with NOT LIKE fixed value",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "name",
						Value:    sql.NewValue("admin%"),
						Operator: sql.NOTLIKE,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE name NOT LIKE 'admin%'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with NOT BETWEEN fixed values",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "age",
						Value:    sql.NewValue([]any{18, 65}),
						Operator: sql.NOTBETWEEN,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE age NOT BETWEEN 18 AND 65",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with REGEXP fixed value",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Field:    "email",
						Value:    sql.NewValue("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
						Operator: sql.REGEXP,
					},
				},
				lastIndex: new(int),
			},
			want:    "WHERE email REGEXP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with invalid limit value",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue("not-an-int"),
				},
				lastIndex: new(int),
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
				lastIndex: new(int),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with zero limit value",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue(int64(0)),
				},
				lastIndex: new(int),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with negative limit value",
			args: args{
				filter: &sql.Filter{
					Limit: sql.NewValue(int64(-5)),
				},
				lastIndex: new(int),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with complex nested conditions",
			args: args{
				filter: &sql.Filter{
					Condition: &sql.Condition{
						Operator: sql.AND,
						Conditions: []sql.Condition{
							{
								Field:    "is_active",
								Value:    sql.NewValue(1),
								Operator: sql.EQ,
							},
							{
								Operator: sql.OR,
								Conditions: []sql.Condition{
									{
										Field:    "role",
										Value:    sql.NewValue("admin"),
										Operator: sql.EQ,
									},
									{
										Field:    "role",
										Value:    sql.NewValue("user"),
										Operator: sql.EQ,
									},
								},
							},
							{
								Operator: sql.NOT,
								Conditions: []sql.Condition{
									{
										Field:    "email",
										Value:    sql.NewValue("test@example.com"),
										Operator: sql.EQ,
									},
								},
							},
						},
					},
					GroupBy: sql.NewGroupBy("department"),
					Sort:    sql.NewSort().Add("created_at", sql.Desc).Add("name", sql.Asc),
					Limit:   sql.NewValue(int64(20)),
					Offset:  sql.NewValue(int64(5)),
				},
				lastIndex: new(int),
			},
			want:    "WHERE (is_active = 1 AND (role = 'admin' OR role = 'user') AND NOT (email = 'test@example.com')) GROUP BY (department) ORDER BY created_at DESC, name ASC OFFSET 5 ROWS FETCH NEXT 20 ROWS ONLY",
			want1:   nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseFilter(tt.args.filter, tt.args.lastIndex)
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
