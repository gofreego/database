package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseDeleteByIDQuery(t *testing.T) {
	tests := []struct {
		name    string
		record  sql.Record
		want    string
		wantErr bool
	}{
		{
			name:    "normal user record",
			record:  &records.User{Id: 1, Name: "Alice", Email: "alice@example.com"},
			want:    "DELETE FROM users WHERE id = ?",
			wantErr: false,
		},
		{
			name:    "record with nil table",
			record:  &mockNoTableRecord{},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseDeleteByIDQuery(tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDeleteByIDQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDeleteByIDQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDeleteQuery(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		condition *sql.Condition
		want      string
		want1     []int
		wantErr   bool
	}{
		{
			name:      "simple delete with EQ condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("name", sql.EQ, sql.NewIndexedValue(0)),
			want:      "DELETE FROM users WHERE name = ?",
			want1:     []int{0},
			wantErr:   false,
		},
		{
			name:      "delete with AND condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("name", sql.EQ, sql.NewIndexedValue(0)).And(sql.NewCondition("is_active", sql.EQ, sql.NewIndexedValue(1))),
			want:      "DELETE FROM users WHERE (name = ? AND is_active = ?)",
			want1:     []int{0, 1},
			wantErr:   false,
		},
		{
			name:      "delete with IN condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("id", sql.IN, sql.NewIndexedValue(0).WithCount(3)),
			want:      "DELETE FROM users WHERE id IN (?, ?, ?)",
			want1:     []int{0},
			wantErr:   false,
		},
		{
			name:      "nil table",
			table:     nil,
			condition: sql.NewCondition("name", sql.EQ, sql.NewValue("Alice")),
			want:      "",
			want1:     nil,
			wantErr:   true,
		},
		{
			name:      "invalid condition",
			table:     sql.NewTable("users"),
			condition: &sql.Condition{Field: "", Operator: sql.EQ, Value: sql.NewValue("Alice")},
			want:      "",
			want1:     nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseDeleteQuery(tt.table, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDeleteQuery() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseDeleteQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseSoftDeleteQuery(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		condition *sql.Condition
		want      string
		want1     []int
		wantErr   bool
	}{
		{
			name:      "simple soft delete with EQ condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("name", sql.EQ, sql.NewIndexedValue(0)),
			want:      "UPDATE users SET deleted = 1 WHERE name = ?",
			want1:     []int{0},
			wantErr:   false,
		},
		{
			name:      "soft delete with OR condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("name", sql.EQ, sql.NewIndexedValue(0)).Or(sql.NewCondition("email", sql.EQ, sql.NewIndexedValue(1))),
			want:      "UPDATE users SET deleted = 1 WHERE (name = ? OR email = ?)",
			want1:     []int{0, 1},
			wantErr:   false,
		},
		{
			name:      "soft delete with IS NULL condition",
			table:     sql.NewTable("users"),
			condition: sql.NewCondition("deleted_at", sql.ISNULL, nil),
			want:      "UPDATE users SET deleted = 1 WHERE deleted_at IS NULL",
			want1:     nil,
			wantErr:   false,
		},
		{
			name:      "nil table",
			table:     nil,
			condition: sql.NewCondition("name", sql.EQ, sql.NewValue("Alice")),
			want:      "",
			want1:     nil,
			wantErr:   true,
		},
		{
			name:      "invalid condition",
			table:     sql.NewTable("users"),
			condition: &sql.Condition{Field: "", Operator: sql.EQ, Value: sql.NewValue("Alice")},
			want:      "",
			want1:     nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseSoftDeleteQuery(tt.table, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSoftDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSoftDeleteQuery() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseSoftDeleteQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseSoftDeleteByIDQuery(t *testing.T) {
	tests := []struct {
		name    string
		table   *sql.Table
		record  sql.Record
		want    string
		wantErr bool
	}{
		{
			name:    "normal user record",
			table:   sql.NewTable("users"),
			record:  &records.User{Id: 1, Name: "Alice", Email: "alice@example.com"},
			want:    "UPDATE users SET deleted = 1 WHERE id = ?",
			wantErr: false,
		},
		{
			name:    "nil table",
			table:   nil,
			record:  &records.User{Id: 1, Name: "Alice", Email: "alice@example.com"},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseSoftDeleteByIDQuery(tt.table, tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSoftDeleteByIDQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSoftDeleteByIDQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
