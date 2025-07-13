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
					Add("name", sql.NewValue("John Doe")).
					Add("email", sql.NewValue("john@example.com")),
				condition: &sql.Condition{
					Field:    "id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE users SET name = 'John Doe', email = 'john@example.com' WHERE id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with numeric values",
			args: args{
				table: sql.NewTable("products"),
				updates: sql.NewUpdates().
					Add("price", sql.NewValue(99.99)).
					Add("quantity", sql.NewValue(100)),
				condition: &sql.Condition{
					Field:    "category",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE products SET price = 99.99, quantity = 100 WHERE category = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with boolean values",
			args: args{
				table: sql.NewTable("settings"),
				updates: sql.NewUpdates().
					Add("is_active", sql.NewValue(true)).
					Add("notifications_enabled", sql.NewValue(false)),
				condition: &sql.Condition{
					Field:    "user_id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE settings SET is_active = true, notifications_enabled = false WHERE user_id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with column references",
			args: args{
				table: sql.NewTable("orders"),
				updates: sql.NewUpdates().
					Add("total", sql.NewColumnValue("price * quantity")).
					Add("updated_at", sql.NewValue("NOW()")),
				condition: &sql.Condition{
					Field:    "status",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE orders SET total = price * quantity, updated_at = 'NOW()' WHERE status = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with parameterized values",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewIndexedValue(0)).
					Add("email", sql.NewIndexedValue(1)).
					Add("age", sql.NewIndexedValue(2)),
				condition: &sql.Condition{
					Field:    "id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(3),
				},
			},
			want:    "UPDATE users SET name = ?, email = ?, age = ? WHERE id = ?",
			want1:   []int{0, 1, 2, 3},
			wantErr: false,
		},
		{
			name: "update with complex condition",
			args: args{
				table: sql.NewTable("products"),
				updates: sql.NewUpdates().
					Add("price", sql.NewValue(150.00)).
					Add("discount", sql.NewValue(10)),
				condition: &sql.Condition{
					Operator: sql.AND,
					Conditions: []sql.Condition{
						{
							Field:    "category",
							Operator: sql.EQ,
							Value:    sql.NewIndexedValue(0),
						},
						{
							Field:    "price",
							Operator: sql.GT,
							Value:    sql.NewIndexedValue(1),
						},
					},
				},
			},
			want:    "UPDATE products SET price = 150, discount = 10 WHERE (category = ? AND price > ?)",
			want1:   []int{0, 1},
			wantErr: false,
		},
		{
			name: "update with IN condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("inactive")),
				condition: &sql.Condition{
					Field:    "id",
					Operator: sql.IN,
					Value:    sql.NewIndexedValue(0).WithCount(3),
				},
			},
			want:    "UPDATE users SET status = 'inactive' WHERE id IN (?, ?, ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with LIKE condition",
			args: args{
				table: sql.NewTable("products"),
				updates: sql.NewUpdates().
					Add("category", sql.NewValue("electronics")),
				condition: &sql.Condition{
					Field:    "name",
					Operator: sql.LIKE,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE products SET category = 'electronics' WHERE name LIKE ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with table alias",
			args: args{
				table: &sql.Table{Name: "users", Alias: "u"},
				updates: sql.NewUpdates().
					Add("last_login", sql.NewValue("NOW()")),
				condition: &sql.Condition{
					Field:    "u.id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE users u SET last_login = 'NOW()' WHERE u.id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with joined table",
			args: args{
				table: sql.NewTable("orders").
					WithInnerJoin(sql.NewTable("users"), &sql.Condition{
						Field:    "orders.user_id",
						Operator: sql.EQ,
						Value:    sql.NewColumnValue("users.id"),
					}),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("shipped")),
				condition: &sql.Condition{
					Field:    "users.email",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE orders INNER JOIN users ON orders.user_id = users.id SET status = 'shipped' WHERE users.email = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "nil updates",
			args: args{
				table:     sql.NewTable("users"),
				updates:   nil,
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
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
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
			},
			want:    "UPDATE users SET  WHERE id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with empty field",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("", sql.NewValue("test")),
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "update with nil value",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", nil),
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
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
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "nil table",
			args: args{
				table: nil,
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("test")),
				condition: &sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "invalid condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("test")),
				condition: &sql.Condition{
					Field:    "id",
					Operator: sql.EXISTS, // Unsupported operator
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "update with special characters in values",
			args: args{
				table: sql.NewTable("comments"),
				updates: sql.NewUpdates().
					Add("content", sql.NewValue("It's a \"quoted\" text with 'apostrophes'")).
					Add("author", sql.NewValue("John O'Connor")),
				condition: &sql.Condition{
					Field:    "id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE comments SET content = 'It's a \"quoted\" text with 'apostrophes'', author = 'John O'Connor' WHERE id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with long string values",
			args: args{
				table: sql.NewTable("articles"),
				updates: sql.NewUpdates().
					Add("title", sql.NewValue("This is a very long title that might exceed normal limits")).
					Add("content", sql.NewValue("This is a very long content with multiple paragraphs and lots of text")),
				condition: &sql.Condition{
					Field:    "author_id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE articles SET title = 'This is a very long title that might exceed normal limits', content = 'This is a very long content with multiple paragraphs and lots of text' WHERE author_id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with mixed value types",
			args: args{
				table: sql.NewTable("products"),
				updates: sql.NewUpdates().
					Add("name", sql.NewValue("Product Name")).
					Add("price", sql.NewValue(99.99)).
					Add("is_active", sql.NewValue(true)).
					Add("tags", sql.NewValue("tag1,tag2,tag3")).
					Add("rating", sql.NewValue(4.5)),
				condition: &sql.Condition{
					Field:    "category_id",
					Operator: sql.EQ,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE products SET name = 'Product Name', price = 99.99, is_active = true, tags = 'tag1,tag2,tag3', rating = 4.5 WHERE category_id = ?",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with OR condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("status", sql.NewValue("inactive")),
				condition: &sql.Condition{
					Operator: sql.OR,
					Conditions: []sql.Condition{
						{
							Field:    "last_login",
							Operator: sql.LT,
							Value:    sql.NewIndexedValue(0),
						},
						{
							Field:    "email_verified",
							Operator: sql.EQ,
							Value:    sql.NewIndexedValue(1),
						},
					},
				},
			},
			want:    "UPDATE users SET status = 'inactive' WHERE (last_login < ? OR email_verified = ?)",
			want1:   []int{0, 1},
			wantErr: false,
		},
		{
			name: "update with BETWEEN condition",
			args: args{
				table: sql.NewTable("orders"),
				updates: sql.NewUpdates().
					Add("discount", sql.NewValue(15)),
				condition: &sql.Condition{
					Field:    "total",
					Operator: sql.BETWEEN,
					Value:    sql.NewIndexedValue(0),
				},
			},
			want:    "UPDATE orders SET discount = 15 WHERE (total BETWEEN ? AND ?)",
			want1:   []int{0},
			wantErr: false,
		},
		{
			name: "update with IS NULL condition",
			args: args{
				table: sql.NewTable("users"),
				updates: sql.NewUpdates().
					Add("email_verified_at", sql.NewValue("NOW()")),
				condition: &sql.Condition{
					Field:    "email_verified_at",
					Operator: sql.ISNULL,
				},
			},
			want:    "UPDATE users SET email_verified_at = 'NOW()' WHERE email_verified_at IS NULL",
			want1:   nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseUpdateQuery(tt.args.table, tt.args.updates, tt.args.condition)
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
