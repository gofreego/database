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
			want:    "UPDATE users SET name = 'Alice Updated', email = 'alice.updated@example.com' WHERE id = $1",
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
			want:    "UPDATE users SET is_active = 0, updated_at = 123456789 WHERE (email LIKE $1 AND is_active = 1)",
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
