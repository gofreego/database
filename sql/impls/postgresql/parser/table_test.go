package parser

import (
	"testing"

	"github.com/gofreego/database/sql"
)

func Test_parseTableName(t *testing.T) {
	tests := []struct {
		name    string
		table   *sql.Table
		want    string
		wantErr bool
	}{
		{
			name:    "simple table",
			table:   sql.NewTable("users"),
			want:    "users",
			wantErr: false,
		},
		{
			name:    "table with alias",
			table:   func() *sql.Table { t := sql.NewTable("users"); t.Alias = "u"; return t }(),
			want:    "users u",
			wantErr: false,
		},
		{
			name: "table with INNER JOIN",
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
			want:    "users INNER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name: "table with LEFT JOIN",
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
			want:    "users LEFT JOIN profiles ON users.id = profiles.user_id",
			wantErr: false,
		},
		{
			name: "table with RIGHT JOIN",
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
			want:    "users RIGHT JOIN logins ON users.id = logins.user_id",
			wantErr: false,
		},
		{
			name: "table with multiple joins",
			table: func() *sql.Table {
				t1 := sql.NewTable("users")
				t2 := sql.NewTable("orders")
				t3 := sql.NewTable("products")
				t1.WithInnerJoin(t2, &sql.Condition{
					Field:    "users.id",
					Value:    sql.NewColumnValue("orders.user_id"),
					Operator: sql.EQ,
				})
				t1.WithLeftJoin(t3, &sql.Condition{
					Field:    "orders.product_id",
					Value:    sql.NewColumnValue("products.id"),
					Operator: sql.EQ,
				})
				return t1
			}(),
			want:    "users INNER JOIN orders ON users.id = orders.user_id LEFT JOIN products ON orders.product_id = products.id",
			wantErr: false,
		},
		{
			name:    "empty table name",
			table:   sql.NewTable(""),
			want:    "",
			wantErr: false,
		},
		{
			name:    "nil table",
			table:   nil,
			want:    "",
			wantErr: true,
		},
		{
			name: "table with join error",
			table: func() *sql.Table {
				t1 := sql.NewTable("users")
				t2 := sql.NewTable("orders")
				t1.WithInnerJoin(t2, &sql.Condition{
					Field:    "",
					Value:    sql.NewColumnValue("orders.user_id"),
					Operator: sql.EQ,
				})
				return t1
			}(),
			want:    "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastIndex := 0
			got, err := parseTableName(tt.table, &lastIndex)
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

func Test_getAlias(t *testing.T) {
	tests := []struct {
		name  string
		alias string
		want  string
	}{
		{
			name:  "empty alias",
			alias: "",
			want:  "",
		},
		{
			name:  "simple alias",
			alias: "u",
			want:  " u",
		},
		{
			name:  "long alias",
			alias: "user_table",
			want:  " user_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getAlias(tt.alias)
			if got != tt.want {
				t.Errorf("getAlias() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJoin(t *testing.T) {
	tests := []struct {
		name    string
		join    []sql.Join
		want    string
		wantErr bool
	}{
		{
			name:    "empty joins",
			join:    []sql.Join{},
			want:    "",
			wantErr: false,
		},
		{
			name: "single INNER JOIN",
			join: []sql.Join{
				{
					Type:  sql.InnerJoin,
					Table: sql.NewTable("orders"),
					On: &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    " INNER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name: "single LEFT JOIN",
			join: []sql.Join{
				{
					Type:  sql.LeftJoin,
					Table: sql.NewTable("profiles"),
					On: &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("profiles.user_id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    " LEFT JOIN profiles ON users.id = profiles.user_id",
			wantErr: false,
		},
		{
			name: "single RIGHT JOIN",
			join: []sql.Join{
				{
					Type:  sql.RightJoin,
					Table: sql.NewTable("logins"),
					On: &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("logins.user_id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    " RIGHT JOIN logins ON users.id = logins.user_id",
			wantErr: false,
		},
		{
			name: "multiple joins",
			join: []sql.Join{
				{
					Type:  sql.InnerJoin,
					Table: sql.NewTable("orders"),
					On: &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					},
				},
				{
					Type:  sql.LeftJoin,
					Table: sql.NewTable("products"),
					On: &sql.Condition{
						Field:    "orders.product_id",
						Value:    sql.NewColumnValue("products.id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    " INNER JOIN orders ON users.id = orders.user_id LEFT JOIN products ON orders.product_id = products.id",
			wantErr: false,
		},
		{
			name: "join with invalid condition",
			join: []sql.Join{
				{
					Type:  sql.InnerJoin,
					Table: sql.NewTable("orders"),
					On: &sql.Condition{
						Field:    "",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "join with nil table",
			join: []sql.Join{
				{
					Type:  sql.InnerJoin,
					Table: nil,
					On: &sql.Condition{
						Field:    "users.id",
						Value:    sql.NewColumnValue("orders.user_id"),
						Operator: sql.EQ,
					},
				},
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastIndex := 0
			got, err := parseJoin(tt.join, &lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJoin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseJoin() = %v, want %v", got, tt.want)
			}
		})
	}
}
