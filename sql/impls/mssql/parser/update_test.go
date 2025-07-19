package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

func TestParseUpdateQuery(t *testing.T) {
	type args struct {
		table     *sql.Table
		updates   *sql.Updates
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
			name: "simple update with indexed condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("Alice Updated")).
					Add("email", sql.NewValue("alice.updated@example.com")),
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "UPDATE users SET name = 'Alice Updated', email = 'alice.updated@example.com' WHERE id = @p1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with complex condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("is_active", sql.NewValue(0)).
					Add("updated_at", sql.NewValue(int64(123456789))),
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "email",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.LIKE,
						},
						{
							Field:    "is_active",
							Value:    sql.NewValue(1),
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "UPDATE users SET is_active = 0, updated_at = 123456789 WHERE (email LIKE @p1 AND is_active = 1)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with nil condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("updated_at", sql.NewValue(int64(987654321))),
				condition: nil,
			},
			want:    "UPDATE users SET updated_at = 987654321 WHERE 1=1",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "update with nil table",
			args: args{
				table: nil,
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("Test")),
				condition: &sql.Condition{
					Field:    "id",
					Value:    sql.NewValue(1),
					Operator: sql.EQ,
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseUpdateQuery(tt.args.table, tt.args.updates, tt.args.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpdateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseUpdateQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseUpdateQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseUpdateQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		updates   *sql.Updates
		condition *sql.Condition
		wantErr   bool
	}{
		{
			name:      "nil updates",
			table:     &sql.Table{Name: "users"},
			updates:   nil,
			condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewValue(1)},
			wantErr:   true,
		},
		{
			name:      "nil table",
			table:     nil,
			updates:   sql.NewUpdates().Add("name", sql.NewValue("test")),
			condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewValue(1)},
			wantErr:   true,
		},
		{
			name:      "nil condition",
			table:     &sql.Table{Name: "users"},
			updates:   sql.NewUpdates().Add("name", sql.NewValue("test")),
			condition: nil,
			wantErr:   false, // Should work with nil condition
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := &parser{}
			_, _, err := parser.ParseUpdateQuery(tt.table, tt.updates, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpdateQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseUpdatesEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		updates   *sql.Updates
		lastIndex int
		wantErr   bool
	}{
		{
			name: "empty field",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "", Value: sql.NewValue("test")},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
		{
			name: "nil value",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: nil},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
		{
			name: "column value with empty value",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: &sql.Value{Type: sql.Column, Value: ""}},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
		{
			name: "column value with nil value",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: &sql.Value{Type: sql.Column, Value: nil}},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseUpdates(tt.updates, &tt.lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUpdates() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseUpdatesAdditionalCases(t *testing.T) {
	tests := []struct {
		name      string
		updates   *sql.Updates
		lastIndex int
		want      string
		wantErr   bool
	}{
		{
			name: "multiple updates with mixed types",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: sql.NewValue("John")},
					{Field: "age", Value: sql.NewIndexedValue(0)},
					{Field: "email", Value: sql.NewColumnValue("users.email")},
				},
			},
			lastIndex: 0,
			want:      "name = 'John', age = @p1, email = users.email",
			wantErr:   false,
		},
		{
			name: "single update with indexed value",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: sql.NewIndexedValue(0)},
				},
			},
			lastIndex: 0,
			want:      "name = @p1",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := parseUpdates(tt.updates, &tt.lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUpdates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseUpdates() = %v, want %v", got, tt.want)
			}
		})
	}
}
