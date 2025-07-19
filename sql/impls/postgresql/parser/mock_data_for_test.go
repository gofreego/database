package parser

import "github.com/gofreego/database/sql"

type mockNoTableRecord struct{}

func (m *mockNoTableRecord) ID() int64               { return 1 }
func (m *mockNoTableRecord) IdColumn() string        { return "id" }
func (m *mockNoTableRecord) SetID(id int64)          {}
func (m *mockNoTableRecord) Table() *sql.Table       { return nil }
func (m *mockNoTableRecord) Columns() []*sql.Field   { return []*sql.Field{sql.NewField("id")} }
func (m *mockNoTableRecord) Values() []any           { return []any{} }
func (m *mockNoTableRecord) Scan(row sql.Row) error  { return nil }
func (m *mockNoTableRecord) SetDeleted(deleted bool) {}

type mockIdOnlyRecord struct {
	Id int64
}

func (m *mockIdOnlyRecord) ID() int64               { return m.Id }
func (m *mockIdOnlyRecord) IdColumn() string        { return "id" }
func (m *mockIdOnlyRecord) SetID(id int64)          { m.Id = id }
func (m *mockIdOnlyRecord) Table() *sql.Table       { return sql.NewTable("mock") }
func (m *mockIdOnlyRecord) Columns() []*sql.Field   { return []*sql.Field{sql.NewField("id")} }
func (m *mockIdOnlyRecord) Values() []any           { return []any{} }
func (m *mockIdOnlyRecord) Scan(row sql.Row) error  { return nil }
func (m *mockIdOnlyRecord) SetDeleted(deleted bool) {}
