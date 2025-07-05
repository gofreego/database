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
		want1   []*sql.Value
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
			want1:   []*sql.Value{sql.NewIndexedValue(0)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithCount(3).WithType(sql.Array)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithCount(3).WithType(sql.Array)},
			wantErr: false,
		},
		{
			name: "test with LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0).WithType(sql.String),
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithType(sql.String)},
			wantErr: false,
		},
		{
			name: "test with empty LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0).WithType(sql.String),
					Operator: sql.LIKE,
				},
			},
			want:    "name LIKE ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithType(sql.String)},
			wantErr: false,
		},
		{
			name: "test with NOT LIKE condition",
			args: args{
				condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewIndexedValue(0).WithType(sql.String),
					Operator: sql.NOTLIKE,
				},
			},
			want:    "name NOT LIKE ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithType(sql.String)},
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
					Value:    sql.NewIndexedValue(0).WithType(sql.String),
					Operator: sql.REGEXP,
				},
			},
			want:    "name REGEXP ?",
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithType(sql.String)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithCount(2).WithType(sql.Array)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0).WithCount(2).WithType(sql.Array)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0), sql.NewIndexedValue(1)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0), sql.NewIndexedValue(1)},
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
			want1:   []*sql.Value{sql.NewIndexedValue(0)},
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
