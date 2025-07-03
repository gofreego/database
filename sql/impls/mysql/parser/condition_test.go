package parser

import (
	"reflect"
	"testing"

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
		want1   []any
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
					Value:    "John",
					Operator: sql.EQ,
				},
			},
			want:    "name = ?",
			want1:   []any{"John"},
			wantErr: false,
		},
		{
			name: "test with IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    []any{1, 2, 3},
					Operator: sql.IN,
				},
			},
			want:    "id IN (?, ?, ?)",
			want1:   []any{1, 2, 3},
			wantErr: false,
		},
		{
			name: "test with empty IN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "id",
					Value:    []any{},
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
					Value:    []any{1, 2, 3},
					Operator: sql.NOTIN,
				},
			},
			want:    "id NOT IN (?, ?, ?)",
			want1:   []any{1, 2, 3},
			wantErr: false,
		},
		{
			name: "test with LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    "John%",
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE ?",
			want1:   []any{"John%"},
			wantErr: false,
		},
		{
			name: "test with empty LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    "",
					Operator: sql.LIKE,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: " test Like with invalid value type",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    123, // Invalid type for LIKE
					Operator: sql.LIKE,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true, // Expecting an error due to invalid value type
		},
		{
			name: "test with NOT LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    "John%",
					Operator: sql.NOTLIKE,
				},
			},
			want:    "name NOT LIKE ?",
			want1:   []any{"John%"},
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
							Value:    1,
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
							Value:    1,
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
					Value:    "^[A-Z].*",
					Operator: sql.REGEXP,
				},
			},
			want:    "name REGEXP ?",
			want1:   []any{"^[A-Z].*"},
			wantErr: false,
		},
		{
			name: "test with BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    []any{18, 30},
					Operator: sql.BETWEEN,
				},
			},
			want:    "(age BETWEEN ? AND ?)",
			want1:   []any{18, 30},
			wantErr: false,
		},
		{
			name: "test with NOT BETWEEN condition",
			args: args{
				condition: &sql.Condition{
					Field:    "age",
					Value:    []any{18, 30},
					Operator: sql.NOTBETWEEN,
				},
			},
			want:    "(age NOT BETWEEN ? AND ?)",
			want1:   []any{18, 30},
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
							Value:    "John",
							Operator: sql.EQ,
						},
						{
							Field:    "age",
							Value:    30,
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "(name = ? AND age > ?)",
			want1:   []any{"John", 30},
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
							Value:    "John",
							Operator: sql.EQ,
						},
						{
							Field:    "age",
							Value:    30,
							Operator: sql.GT,
						},
					},
				},
			},
			want:    "(name = ? OR age > ?)",
			want1:   []any{"John", 30},
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
							Value:    "John",
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "NOT (name = ?)",
			want1:   []any{"John"},
			wantErr: false,
		},
		{
			name: "test with invalid operator",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    "John",
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
					Value:    "John",
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
					Value:    []any{},
					Operator: sql.IN,
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
