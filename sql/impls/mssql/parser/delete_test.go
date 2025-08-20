package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseDeleteByIDQuery(t *testing.T) {
	type args struct {
		record sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test delete by id with valid record",
			args: args{
				record: &records.User{Id: 1},
			},
			want:    "DELETE FROM users WHERE id = @p1",
			wantErr: false,
		},
		{
			name: "test delete by id with nil table",
			args: args{
				record: &mockNoTableRecord{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseDeleteByIDQuery(tt.args.record)
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
	type args struct {
		table     *sql.Table
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
			name: "test delete with simple condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "status",
					Value:    sql.NewValue("inactive"),
					Operator: sql.EQ,
				},
			},
			want:    "DELETE FROM users WHERE status = 'inactive'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test delete with indexed condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "email",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "DELETE FROM users WHERE email = @p1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test delete with complex condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "is_active",
							Value:    sql.NewValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "last_login",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.LT,
						},
					},
				},
			},
			want:    "DELETE FROM users WHERE (is_active = 0 AND last_login < @p1)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test delete with nil condition",
			args: args{
				table:     sql.NewTable("users"),
				condition: nil,
			},
			want:    "DELETE FROM users WHERE 1=1",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test delete with nil table",
			args: args{
				table: nil,
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
		{
			name: "test delete with LIKE condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "email",
					Value:    sql.NewValue("test%"),
					Operator: sql.LIKE,
				},
			},
			want:    "DELETE FROM users WHERE email LIKE 'test%'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test delete with IN condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "status",
					Value:    sql.NewValue([]any{"deleted", "archived"}),
					Operator: sql.IN,
				},
			},
			want:    "DELETE FROM users WHERE status IN ('deleted', 'archived')",
			want1:   nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseDeleteQuery(tt.args.table, tt.args.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDeleteQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseDeleteQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseSoftDeleteByIDQuery(t *testing.T) {
	type args struct {
		table  *sql.Table
		record sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test soft delete by id with valid record",
			args: args{
				table:  sql.NewTable("users"),
				record: &records.User{Id: 1},
			},
			want:    "UPDATE users SET deleted = 1 WHERE id = @p1",
			wantErr: false,
		},
		{
			name: "test soft delete by id with nil table",
			args: args{
				table:  nil,
				record: &records.User{Id: 1},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test soft delete by id with nil record table",
			args: args{
				table:  sql.NewTable("users"),
				record: &mockNoTableRecord{},
			},
			want:    "UPDATE users SET deleted = 1 WHERE id = @p1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseSoftDeleteByIDQuery(tt.args.table, tt.args.record)
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

func TestParseSoftDeleteQuery(t *testing.T) {
	type args struct {
		table     *sql.Table
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
			name: "test soft delete with simple condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "status",
					Value:    sql.NewValue("inactive"),
					Operator: sql.EQ,
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE status = 'inactive'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test soft delete with indexed condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "email",
					Value:    sql.NewIndexedValue(0),
					Operator: sql.EQ,
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE email = @p1",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test soft delete with complex condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Operator: sql.OR,
					Conditions: []sql.Condition{
						{
							Field:    "is_active",
							Value:    sql.NewValue(0),
							Operator: sql.EQ,
						},
						{
							Field:    "last_login",
							Value:    sql.NewIndexedValue(0),
							Operator: sql.LT,
						},
					},
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE (is_active = 0 OR last_login < @p1)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "test soft delete with nil condition",
			args: args{
				table:     sql.NewTable("users"),
				condition: nil,
			},
			want:    "UPDATE users SET deleted = 1 WHERE 1=1",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test soft delete with nil table",
			args: args{
				table: nil,
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
		{
			name: "test soft delete with NOT condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Operator: sql.NOT,
					Conditions: []sql.Condition{
						{
							Field:    "is_admin",
							Value:    sql.NewValue(1),
							Operator: sql.EQ,
						},
					},
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE NOT (is_admin = 1)",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test soft delete with BETWEEN condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "created_at",
					Value:    sql.NewValue([]any{"2023-01-01", "2023-12-31"}),
					Operator: sql.BETWEEN,
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'",
			want1:   nil,
			wantErr: false,
		},
		{
			name: "test soft delete with IS NULL condition",
			args: args{
				table: sql.NewTable("users"),
				condition: &sql.Condition{
					Field:    "email",
					Operator: sql.ISNULL,
				},
			},
			want:    "UPDATE users SET deleted = 1 WHERE email IS NULL",
			want1:   nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseSoftDeleteQuery(tt.args.table, tt.args.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSoftDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSoftDeleteQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseSoftDeleteQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseDeleteQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		condition *sql.Condition
		wantErr   bool
	}{
		{
			name:      "nil table with condition",
			table:     nil,
			condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewValue(1)},
			wantErr:   true,
		},
		{
			name:      "nil table with nil condition",
			table:     nil,
			condition: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := &parser{}
			_, _, err := parser.ParseDeleteQuery(tt.table, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseSoftDeleteQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		condition *sql.Condition
		wantErr   bool
	}{
		{
			name:      "nil table with condition",
			table:     nil,
			condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewValue(1)},
			wantErr:   true,
		},
		{
			name:      "nil table with nil condition",
			table:     nil,
			condition: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := &parser{}
			_, _, err := parser.ParseSoftDeleteQuery(tt.table, tt.condition)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSoftDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
