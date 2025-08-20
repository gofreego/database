package parser

import (
	"testing"

	"github.com/gofreego/database/sql"
)

func Test_parseTableName(t *testing.T) {
	type args struct {
		table *sql.Table
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "simple table",
			args: args{
				table: sql.NewTable("users"),
			},
			want:    "users",
			wantErr: false,
		},
		{
			name: "table with alias",
			args: args{
				table: func() *sql.Table {
					t := sql.NewTable("users")
					t.Alias = "u"
					return t
				}(),
			},
			want:    "users u",
			wantErr: false,
		},
		{
			name: "table with INNER JOIN",
			args: args{
				table: func() *sql.Table {
					t1 := sql.NewTable("users")
					t2 := sql.NewTable("orders")
					t1.WithInnerJoin(t2, &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					})
					return t1
				}(),
			},
			want:    "users INNER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name: "table with LEFT JOIN",
			args: args{
				table: func() *sql.Table {
					t1 := sql.NewTable("users")
					t2 := sql.NewTable("profiles")
					t1.WithLeftJoin(t2, &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("profiles.user_id"),
						Operator: sql.EQ,
					})
					return t1
				}(),
			},
			want:    "users LEFT JOIN profiles ON users.id = profiles.user_id",
			wantErr: false,
		},
		{
			name: "table with RIGHT JOIN",
			args: args{
				table: func() *sql.Table {
					t1 := sql.NewTable("users")
					t2 := sql.NewTable("logins")
					t1.WithRightJoin(t2, &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("logins.user_id"),
						Operator: sql.EQ,
					})
					return t1
				}(),
			},
			want:    "users RIGHT JOIN logins ON users.id = logins.user_id",
			wantErr: false,
		},
		{
			name: "table with multiple joins",
			args: args{
				table: func() *sql.Table {
					t1 := sql.NewTable("users")
					t2 := sql.NewTable("orders")
					t3 := sql.NewTable("payments")
					t1.WithInnerJoin(t2, &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					})
					t1.WithLeftJoin(t3, &sql.Condition{
						Field:    "orders.id",
						Value:    sql.NewColumnValue("payments.order_id"),
						Operator: sql.EQ,
					})
					return t1
				}(),
			},
			want:    "users INNER JOIN orders ON users.id = orders.user_id LEFT JOIN payments ON orders.id = payments.order_id",
			wantErr: false,
		},
		{
			name: "empty table name",
			args: args{
				table: sql.NewTable(""),
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "nil table",
			args: args{
				table: nil,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTableName(tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseTableName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}
