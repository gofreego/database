package parser

import (
	"testing"
	"time"

	"github.com/gofreego/database/sql"
)

func TestParseConditionEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		condition *sql.Condition
		lastIndex int
		want      string
		wantErr   bool
	}{
		{
			name: "invalid condition validation",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.EQ,
				// Missing Value should cause validation error
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "column comparison with non-string value",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.EQ,
				Value:    &sql.Value{Type: sql.Column, Value: 123}, // Non-string value for column comparison
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "IN operator with empty slice",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.IN,
				Value:    sql.NewValue([]any{}),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "IN operator with non-slice value",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.IN,
				Value:    sql.NewValue("not a slice"),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "IN operator with zero count",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.IN,
				Value:    &sql.Value{Count: 0},
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "LIKE operator with empty string",
			condition: &sql.Condition{
				Field:    "name",
				Operator: sql.LIKE,
				Value:    sql.NewValue(""),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "LIKE operator with non-string value",
			condition: &sql.Condition{
				Field:    "name",
				Operator: sql.LIKE,
				Value:    sql.NewValue(123),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "EXISTS operator not implemented",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.EXISTS,
				Value:    sql.NewValue("subquery"),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "NOTEXISTS operator not implemented",
			condition: &sql.Condition{
				Field:    "id",
				Operator: sql.NOTEXISTS,
				Value:    sql.NewValue("subquery"),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "AND operator with no valid sub-conditions",
			condition: &sql.Condition{
				Operator: sql.AND,
				Conditions: []sql.Condition{
					{
						Field:    "id",
						Operator: sql.EQ,
						Value:    &sql.Value{Type: sql.Column, Value: 123}, // This will cause validation error
					},
				},
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "OR operator with no valid sub-conditions",
			condition: &sql.Condition{
				Operator: sql.OR,
				Conditions: []sql.Condition{
					{
						Field:    "id",
						Operator: sql.EQ,
						Value:    &sql.Value{Type: sql.Column, Value: 123}, // This will cause validation error
					},
				},
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "NOT operator with wrong number of sub-conditions",
			condition: &sql.Condition{
				Operator: sql.NOT,
				Conditions: []sql.Condition{
					{
						Field:    "id",
						Operator: sql.EQ,
						Value:    sql.NewValue("value1"),
					},
					{
						Field:    "name",
						Operator: sql.EQ,
						Value:    sql.NewValue("value2"),
					},
				},
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "NOT operator with invalid sub-condition",
			condition: &sql.Condition{
				Operator: sql.NOT,
				Conditions: []sql.Condition{
					{
						Field:    "id",
						Operator: sql.EQ,
						Value:    &sql.Value{Type: sql.Column, Value: 123}, // This will cause validation error
					},
				},
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "BETWEEN operator with wrong slice length",
			condition: &sql.Condition{
				Field:    "age",
				Operator: sql.BETWEEN,
				Value:    sql.NewValue([]any{1}), // Only one value, need two
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "BETWEEN operator with non-slice value",
			condition: &sql.Condition{
				Field:    "age",
				Operator: sql.BETWEEN,
				Value:    sql.NewValue("not a slice"),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "invalid operator",
			condition: &sql.Condition{
				Field:    "id",
				Operator: 999, // Invalid operator
				Value:    sql.NewValue("value"),
			},
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := parseCondition(tt.condition, &tt.lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetValue(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			name:  "string value",
			value: "test",
			want:  "'test'",
		},
		{
			name:  "time value",
			value: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			want:  "'2023-01-01T12:00:00Z'",
		},
		{
			name:  "int value",
			value: 123,
			want:  "123",
		},
		{
			name:  "float value",
			value: 123.45,
			want:  "123.45",
		},
		{
			name:  "bool value",
			value: true,
			want:  "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getValue(tt.value)
			if got != tt.want {
				t.Errorf("getValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
