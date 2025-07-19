package parser

import (
	"testing"

	"github.com/gofreego/database/sql"
)

func TestParseField(t *testing.T) {
	tests := []struct {
		name  string
		field *sql.Field
		want  string
	}{
		{
			name: "simple field",
			field: &sql.Field{
				Name: "id",
			},
			want: "id",
		},
		{
			name: "field with alias",
			field: &sql.Field{
				Name:  "name",
				Alias: "user_name",
			},
			want: "name AS user_name",
		},
		{
			name: "field with distinct",
			field: &sql.Field{
				Name:     "email",
				Distinct: true,
			},
			want: "DISTINCT email",
		},
		{
			name: "field with function",
			field: &sql.Field{
				Name: "age",
				Func: sql.Count,
			},
			want: "COUNT(age)",
		},
		{
			name: "field with distinct and function",
			field: &sql.Field{
				Name:     "age",
				Distinct: true,
				Func:     sql.Count,
			},
			want: "COUNT(DISTINCT age)",
		},
		{
			name: "field with function and alias",
			field: &sql.Field{
				Name:  "age",
				Func:  sql.Avg,
				Alias: "average_age",
			},
			want: "AVG(age) AS average_age",
		},
		{
			name: "field with distinct, function and alias",
			field: &sql.Field{
				Name:     "age",
				Distinct: true,
				Func:     sql.Sum,
				Alias:    "total_age",
			},
			want: "SUM(DISTINCT age) AS total_age",
		},
		{
			name: "nested field",
			field: &sql.Field{
				Field: &sql.Field{
					Name: "sub_field",
				},
			},
			want: "sub_field",
		},
		{
			name: "nested field with distinct",
			field: &sql.Field{
				Field: &sql.Field{
					Name: "sub_field",
				},
				Distinct: true,
			},
			want: "DISTINCT sub_field",
		},
		{
			name: "nested field with function",
			field: &sql.Field{
				Field: &sql.Field{
					Name: "sub_field",
				},
				Func: sql.Max,
			},
			want: "MAX(sub_field)",
		},
		{
			name: "nested field with distinct, function and alias",
			field: &sql.Field{
				Field: &sql.Field{
					Name: "sub_field",
				},
				Distinct: true,
				Func:     sql.Min,
				Alias:    "min_value",
			},
			want: "MIN(DISTINCT sub_field) AS min_value",
		},
		{
			name:  "empty field",
			field: &sql.Field{},
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseField(tt.field)
			if got != tt.want {
				t.Errorf("parseField() = %v, want %v", got, tt.want)
			}
		})
	}
}
