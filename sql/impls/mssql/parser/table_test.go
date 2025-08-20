package parser

import (
	"testing"

	"github.com/gofreego/database/sql"
)

func TestParseTableName(t *testing.T) {
	tests := []struct {
		name      string
		table     *sql.Table
		lastIndex int
		want      string
		wantErr   bool
	}{
		{
			name:      "nil table",
			table:     nil,
			lastIndex: 0,
			want:      "",
			wantErr:   true,
		},
		{
			name: "simple table",
			table: &sql.Table{
				Name: "users",
			},
			lastIndex: 0,
			want:      "users",
			wantErr:   false,
		},
		{
			name: "table with alias",
			table: &sql.Table{
				Name:  "users",
				Alias: "u",
			},
			lastIndex: 0,
			want:      "users u",
			wantErr:   false,
		},
		{
			name: "table with join",
			table: &sql.Table{
				Name: "users",
				Join: []sql.Join{
					{
						Type: sql.InnerJoin,
						Table: &sql.Table{
							Name: "orders",
						},
						On: &sql.Condition{
							Field:    "users.id",
							Operator: sql.EQ,
							Value:    sql.NewColumnValue("orders.user_id"),
						},
					},
				},
			},
			lastIndex: 0,
			want:      "users INNER JOIN orders ON users.id = orders.user_id",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTableName(tt.table, &tt.lastIndex)
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

func TestGetAlias(t *testing.T) {
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
			name:  "non-empty alias",
			alias: "u",
			want:  " u",
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

func TestParseJoin(t *testing.T) {
	tests := []struct {
		name      string
		joins     []sql.Join
		lastIndex int
		want      string
		wantErr   bool
	}{
		{
			name:      "empty joins",
			joins:     []sql.Join{},
			lastIndex: 0,
			want:      "",
			wantErr:   false,
		},
		{
			name: "single join",
			joins: []sql.Join{
				{
					Type: sql.InnerJoin,
					Table: &sql.Table{
						Name: "orders",
					},
					On: &sql.Condition{
						Field:    "users.id",
						Operator: sql.EQ,
						Value:    sql.NewColumnValue("orders.user_id"),
					},
				},
			},
			lastIndex: 0,
			want:      " INNER JOIN orders ON users.id = orders.user_id",
			wantErr:   false,
		},
		{
			name: "multiple joins",
			joins: []sql.Join{
				{
					Type: sql.LeftJoin,
					Table: &sql.Table{
						Name: "orders",
					},
					On: &sql.Condition{
						Field:    "users.id",
						Operator: sql.EQ,
						Value:    sql.NewColumnValue("orders.user_id"),
					},
				},
				{
					Type: sql.RightJoin,
					Table: &sql.Table{
						Name: "payments",
					},
					On: &sql.Condition{
						Field:    "orders.id",
						Operator: sql.EQ,
						Value:    sql.NewColumnValue("payments.order_id"),
					},
				},
			},
			lastIndex: 0,
			want:      " LEFT JOIN orders ON users.id = orders.user_id RIGHT JOIN payments ON orders.id = payments.order_id",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJoin(tt.joins, &tt.lastIndex)
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

func TestParseJoinEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		joins     []sql.Join
		lastIndex int
		wantErr   bool
	}{
		{
			name: "join with invalid condition",
			joins: []sql.Join{
				{
					Type: sql.InnerJoin,
					Table: &sql.Table{
						Name: "orders",
					},
					On: &sql.Condition{
						Field:    "users.id",
						Operator: sql.EQ,
						Value:    &sql.Value{Type: sql.Column, Value: 123}, // Invalid column value
					},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
		{
			name: "join with invalid table",
			joins: []sql.Join{
				{
					Type:  sql.InnerJoin,
					Table: nil, // Invalid table
					On: &sql.Condition{
						Field:    "users.id",
						Operator: sql.EQ,
						Value:    sql.NewColumnValue("orders.user_id"),
					},
				},
			},
			lastIndex: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseJoin(tt.joins, &tt.lastIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJoin() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
