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
			name: "normal update with string values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("John")).
					Add("email", sql.NewValue("john@example.com")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with numeric values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("age", sql.NewValue(25)).
					Add("score", sql.NewValue(100.5)),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET age = 25, score = 100.5 WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with boolean values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("is_active", sql.NewValue(true)).
					Add("is_verified", sql.NewValue(false)),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET is_active = true, is_verified = false WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with column references",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("last_updated", sql.NewColumnValue("updated_at")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET last_updated = updated_at WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with parameterized values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewIndexedValue(0)).
					Add("email", sql.NewIndexedValue(1)),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(2)),
			},
			want:    "UPDATE users SET name = $1, email = $2 WHERE id = $3",
			want1:   []int{0, 1, 2},
			wantErr: false,
		},
		{
			name: "update with complex condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("active")),
				condition: sql.NewCondition("age", sql.GT, sql.NewIndexedValue(0)).
					And(sql.NewCondition("is_active", sql.EQ, sql.NewValue(true))),
			},
			want:    "UPDATE users SET status = 'active' WHERE (age > $1 AND is_active = true)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with IN condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("inactive")),
				condition: sql.NewCondition("id", sql.IN, sql.NewIndexedValue(0).WithCount(3)),
			},
			want:    "UPDATE users SET status = 'inactive' WHERE id IN ($1, $2, $3)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with LIKE condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("verified", sql.NewValue(true)),
				condition: sql.NewCondition("email", sql.LIKE, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET verified = true WHERE email LIKE $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with table alias",
			args: args{
				table: func() *sql.Table { t := sql.NewTable("users"); t.Alias = "u"; return t }(),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("John")),
				condition: sql.NewCondition("u.id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users u SET name = 'John' WHERE u.id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with joined table",
			args: args{
				table: func() *sql.Table {
					t1 := sql.NewTable("users")
					t2 := sql.NewTable("profiles")
					t1.WithInnerJoin(t2, &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("profiles.user_id"),
						Operator: sql.EQ,
					})
					return t1
				}(),
				updates: sql.NewUpdates().
					Add("users.name", sql.NewValue("John")),
				condition: sql.NewCondition("profiles.id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users INNER JOIN profiles ON users.id = profiles.user_id SET users.name = 'John' WHERE profiles.id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "nil updates",
			args: args{
				table:     sql.NewTable("users"),
				updates:   nil,
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "empty updates",
			args: args{
				table:     sql.NewTable("users"),
				updates:   sql.NewUpdates(),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET  WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with empty field",
			args: args{
				table: sql.NewTable("users"),
				updates: &sql.Updates{
					Fields: []sql.UpdateField{
						{Field: "", Value: sql.NewValue("John")},
					},
				},
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "update with nil value",
			args: args{
				table: sql.NewTable("users"),
				updates: &sql.Updates{
					Fields: []sql.UpdateField{
						{Field: "name", Value: nil},
					},
				},
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "update with empty column value",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewColumnValue("")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "nil table",
			args: args{
				table:     nil,
				updates:   sql.NewUpdates().Add("name", sql.NewValue("John")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "invalid condition",
			args: args{
				table:     sql.NewTable("users"),
				updates:   sql.NewUpdates().Add("name", sql.NewValue("John")),
				condition: &sql.Condition{Field: "", Operator: sql.EQ, Value: sql.NewValue("Alice")},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "update with special characters in values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("O'Connor")).
					Add("email", sql.NewValue("test@example.com")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET name = 'O'Connor', email = 'test@example.com' WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with long string values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("description", sql.NewValue("This is a very long description that contains many characters")),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET description = 'This is a very long description that contains many characters' WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with mixed value types",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("John")).
					Add("age", sql.NewValue(25)).
					Add("is_active", sql.NewValue(true)).
					Add("score", sql.NewValue(95.5)),
				condition: sql.NewCondition("id", sql.EQ, sql.NewIndexedValue(0)),
			},
			want:    "UPDATE users SET name = 'John', age = 25, is_active = true, score = 95.5 WHERE id = $1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with OR condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("inactive")),
				condition: sql.NewCondition("age", sql.LT, sql.NewIndexedValue(0)).
					Or(sql.NewCondition("is_active", sql.EQ, sql.NewValue(false))),
			},
			want:    "UPDATE users SET status = 'inactive' WHERE (age < $1 OR is_active = false)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with BETWEEN condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("category", sql.NewValue("premium")),
				condition: sql.NewCondition("score", sql.BETWEEN, sql.NewIndexedValue(0).WithCount(2)),
			},
			want:    "UPDATE users SET category = 'premium' WHERE (score BETWEEN $1 AND $2)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with IS NULL condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("verified_at", sql.NewValue("2023-01-01")),
				condition: sql.NewCondition("verified_at", sql.ISNULL, nil),
			},
			want:    "UPDATE users SET verified_at = '2023-01-01' WHERE verified_at IS NULL",
			want1:   nil,
			wantErr: false,
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
				t.Errorf("ParseUpdateQuery() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseUpdateQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_parseUpdates(t *testing.T) {
	tests := []struct {
		name    string
		updates *sql.Updates
		want    string
		want1   []int
		wantErr bool
	}{
		{
			name: "normal updates with string values",
			updates: sql.NewUpdates().
				Add("name", sql.NewValue("John")).
				Add("email", sql.NewValue("john@example.com")),
			want:    "name = 'John', email = 'john@example.com'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "updates with numeric values",
			updates: sql.NewUpdates().
				Add("age", sql.NewValue(25)).
				Add("score", sql.NewValue(100.5)),
			want:    "age = 25, score = 100.5",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "updates with boolean values",
			updates: sql.NewUpdates().
				Add("is_active", sql.NewValue(true)).
				Add("is_verified", sql.NewValue(false)),
			want:    "is_active = true, is_verified = false",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "updates with column references",
			updates: sql.NewUpdates().
				Add("last_updated", sql.NewColumnValue("updated_at")),
			want:    "last_updated = updated_at",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "updates with parameterized values",
			updates: sql.NewUpdates().
				Add("name", sql.NewIndexedValue(0)).
				Add("email", sql.NewIndexedValue(1)),
			want:    "name = $1, email = $2",
			want1:   []int{0, 1},
			wantErr: false,
		},
		{
			name: "empty field",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "", Value: sql.NewValue("John")},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "nil value",
			updates: &sql.Updates{
				Fields: []sql.UpdateField{
					{Field: "name", Value: nil},
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "empty column value",
			updates: sql.NewUpdates().
				Add("name", sql.NewColumnValue("")),
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "mixed value types",
			updates: sql.NewUpdates().
				Add("name", sql.NewValue("John")).
				Add("age", sql.NewValue(25)).
				Add("is_active", sql.NewValue(true)).
				Add("score", sql.NewIndexedValue(0)),
			want:    "name = 'John', age = 25, is_active = true, score = $1",
			want1:   []int{0},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastIndex := 0
			got, got1, err := parseUpdates(tt.updates, &lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUpdates() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseUpdates() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("parseUpdates() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
