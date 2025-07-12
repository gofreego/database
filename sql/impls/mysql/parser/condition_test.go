package parser

import (
	"reflect"
	"testing"
	"time"

	"github.com/gofreego/database/sql"
)

func Test_parseCondition(t *testing.T) {
	type args struct {
		condition *sql.Condition
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []int
		wantErr bool
	}{
		{
			name: "test with nil condition",
			args: args{
				condition: nil,
			},
			want:    "",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with simple condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "name = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewIndexedValue(0).WithCount(3),
					Operator: sql.IN,
				},
			},
			want:    "id IN (?, ?, ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with empty IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewIndexedValue(0).WithCount(0),
					Operator: sql.IN,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with NOT IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewIndexedValue(0).WithCount(3),
					Operator: sql.NOTIN,
				},
			},
			want:    "id NOT IN (?, ?, ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with empty LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with NOT LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.NOTLIKE,
				},
			},
			want:    "name NOT LIKE ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with IS NULL condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Operator: sql.ISNULL,
				},
			},
			want:    "name IS NULL",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with IS NOT NULL condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Operator: sql.ISNOTNULL,
				},
			},
			want:    "name IS NOT NULL",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with EXISTS condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.EXISTS,
					Conditions: []sql.Condition{
						{
							Field:    "id",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true, // EXISTS is not implemented in this parser
		},
		{
			name: "test with NOT EXISTS condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.NOTEXISTS,
					Conditions: []sql.Condition{
						{
							Field:    "id",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true, // NOT EXISTS is not implemented in this parser
		},
		{
			name: "test with REGEXP condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.REGEXP,
				},
			},
			want:    "name REGEXP ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewIndexedValue(0).WithCount(2),
					Operator: sql.BETWEEN,
				},
			},
			want:    "(age BETWEEN ? AND ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with NOT BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewIndexedValue(0).WithCount(2),
					Operator: sql.NOTBETWEEN,
				},
			},
			want:    "(age NOT BETWEEN ? AND ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with AND condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "(name = ? AND age > ?)",
			want1:   []int{0, 1},
			wantErr: false,
		},
		{
			name: "test with OR condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.OR,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "(name = ? OR age > ?)",
			want1:   []int{0, 1},
			wantErr: false,
		},
		{
			name: "test with NOT condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.NOT,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "NOT (name = ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test with invalid operator",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.Operator(999), // Invalid operator
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with empty field",
			args: args{
				condition: &sql.Condition{
					Field:    "",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with empty value for IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewIndexedValue(0).WithCount(0),
					Operator: sql.IN,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		// Additional missing test cases
		{
			name: "test with fixed value for EQ condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewValue(25),
					Operator: sql.EQ,
				},
			},
			want:    "age = 25",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed value for NEQ condition",
			args: args{
				condition: &sql.Condition{
					Field:    "status",
					Value:    sql.NewValue("inactive"),
					Operator: sql.NEQ,
				},
			},
			want:    "status <> 'inactive'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed value for GT condition",
			args: args{
				condition: &sql.Condition{
					Field:    "score",
					Value:    sql.NewValue(100.5),
					Operator: sql.GT,
				},
			},
			want:    "score > 100.5",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed value for GTE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "price",
					Value:    sql.NewValue(99.99),
					Operator: sql.GTE,
				},
			},
			want:    "price >= 99.99",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed value for LT condition",
			args: args{
				condition: &sql.Condition{
					Field:    "quantity",
					Value:    sql.NewValue(50),
					Operator: sql.LT,
				},
			},
			want:    "quantity < 50",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed value for LTE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "weight",
					Value:    sql.NewValue(10.5),
					Operator: sql.LTE,
				},
			},
			want:    "weight <= 10.5",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed slice value for IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "category",
					Value:    sql.NewValue([]any{"electronics", "books", "clothing"}),
					Operator: sql.IN,
				},
			},
			want:    "category IN ('electronics', 'books', 'clothing')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed slice value for NOT IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "status",
					Value:    sql.NewValue([]any{"deleted", "archived"}),
					Operator: sql.NOTIN,
				},
			},
			want:    "status NOT IN ('deleted', 'archived')",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed string value for LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewValue("john%"),
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE 'john%'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed string value for NOT LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "email",
					Value:    sql.NewValue("%spam.com"),
					Operator: sql.NOTLIKE,
				},
			},
			want:    "email NOT LIKE '%spam.com'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed string value for REGEXP condition",
			args: args{
				condition: &sql.Condition{
					Field:    "phone",
					Value:    sql.NewValue("^\\d{3}-\\d{3}-\\d{4}$"),
					Operator: sql.REGEXP,
				},
			},
			want:    "phone REGEXP '^\\d{3}-\\d{3}-\\d{4}$'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed slice value for BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewValue([]any{18, 65}),
					Operator: sql.BETWEEN,
				},
			},
			want:    "age BETWEEN 18 AND 65",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with fixed slice value for NOT BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "price",
					Value:    sql.NewValue([]any{10.0, 100.0}),
					Operator: sql.NOTBETWEEN,
				},
			},
			want:    "price NOT BETWEEN 10 AND 100",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with time value for fixed condition",
			args: args{
				condition: &sql.Condition{
					Field:    "created_at",
					Value:    sql.NewValue(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
					Operator: sql.GT,
				},
			},
			want:    "created_at > '2023-01-01T00:00:00Z'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test with empty string value for LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewValue(""),
					Operator: sql.LIKE,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with non-slice value for IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewValue("not-a-slice"),
					Operator: sql.IN,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with non-string value for LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewValue(123),
					Operator: sql.LIKE,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with slice of wrong length for BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewValue([]any{18}),
					Operator: sql.BETWEEN,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with non-slice value for BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    sql.NewValue("not-a-slice"),
					Operator: sql.BETWEEN,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with AND condition with empty sub-conditions",
			args: args{
				condition: &sql.Condition{
					Operator:   sql.AND,
					Conditions: []sql.Condition{},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with OR condition with empty sub-conditions",
			args: args{
				condition: &sql.Condition{
					Operator:   sql.OR,
					Conditions: []sql.Condition{},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with NOT condition with no sub-conditions",
			args: args{
				condition: &sql.Condition{
					Operator:   sql.NOT,
					Conditions: []sql.Condition{},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with NOT condition with multiple sub-conditions",
			args: args{
				condition: &sql.Condition{
					Operator: sql.NOT,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with complex nested AND/OR conditions",
			args: args{
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "status",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Operator: sql.OR,
							Conditions: []sql.Condition{
								{
									Field:    "age",
									Value:    sql.NewIndexedValue(1),
									Operator: sql.GT,
								},
								{
									Field:    "role",
									Value:    sql.NewIndexedValue(2),
									Operator: sql.EQ,
								},
							},
						},
					},
				},
			},
			want:    "(status = ? AND (age > ? OR role = ?))",
			want1:   []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "test with AND condition with invalid sub-condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "invalid_field",
							Value:    sql.NewIndexedValue(1).WithCount(0),
							Operator: sql.IN,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with NOT condition with invalid sub-condition",
			args: args{
				condition: &sql.Condition{
					Operator: sql.NOT,
					Conditions: []sql.Condition{
						{
							Field:    "invalid_field",
							Value:    sql.NewIndexedValue(0).WithCount(0),
							Operator: sql.IN,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with AND condition with empty sub-condition result",
			args: args{
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "status",
							Value:    sql.NewIndexedValue(1).WithCount(0),
							Operator: sql.IN,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for AND operator with field",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for OR operator with value",
			args: args{
				condition: &sql.Condition{
					Value:    sql.NewIndexedValue(0),
					Operator: sql.OR,
					Conditions: []sql.Condition{
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for EQ operator with conditions",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
					Conditions: []sql.Condition{
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for ISNULL operator with value",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.ISNULL,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for ISNULL operator with conditions",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Operator: sql.ISNULL,
					Conditions: []sql.Condition{
						{
							Field:    "age",
							Value:    sql.NewIndexedValue(1),
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for EQ operator with nil value",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    nil,
					Operator: sql.EQ,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for EQ operator with empty field",
			args: args{
				condition: &sql.Condition{
					Field:    "",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "test with validation error for ISNULL operator with empty field",
			args: args{
				condition: &sql.Condition{
					Field:    "",
					Operator: sql.ISNULL,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseCondition(tt.args.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseCondition() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("parseCondition() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
