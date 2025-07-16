package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

// New local mock for multi-column insert tests

type mockProductRecord struct {
	id    int64
	name  string
	price float64
}

func (m *mockProductRecord) ID() int64               { return m.id }
func (m *mockProductRecord) IdColumn() string        { return "id" }
func (m *mockProductRecord) SetID(id int64)          { m.id = id }
func (m *mockProductRecord) Table() *sql.Table       { return sql.NewTable("products") }
func (m *mockProductRecord) Columns() []string       { return []string{"id", "name", "price"} }
func (m *mockProductRecord) Values() []any           { return []any{m.name, m.price} }
func (m *mockProductRecord) Scan(row sql.Row) error  { return nil }
func (m *mockProductRecord) SetDeleted(deleted bool) {}

func TestParseInsertQuery(t *testing.T) {
	type args struct {
		record []sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []any
		wantErr bool
	}{
		{
			name:    "no records (error)",
			args:    args{record: nil},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name:    "single record, single column (id only)",
			args:    args{record: []sql.Record{&mockIdOnlyRecord{Id: 1}}},
			want:    "INSERT INTO mock () VALUES ()",
			want1:   []any{},
			wantErr: false,
		},
		{
			name:    "multiple records, single column (id only batch)",
			args:    args{record: []sql.Record{&mockIdOnlyRecord{Id: 1}, &mockIdOnlyRecord{Id: 2}}},
			want:    "INSERT INTO mock () VALUES (), ()",
			want1:   []any{},
			wantErr: false,
		},
		{
			name:    "single record, multi column (product)",
			args:    args{record: []sql.Record{&mockProductRecord{id: 1, name: "Widget", price: 9.99}}},
			want:    "INSERT INTO products (name, price) VALUES (@p1, @p2)",
			want1:   []any{"Widget", 9.99},
			wantErr: false,
		},
		{
			name: "multiple records, multi column (product batch)",
			args: args{record: []sql.Record{
				&mockProductRecord{id: 1, name: "Widget", price: 9.99},
				&mockProductRecord{id: 2, name: "Gadget", price: 19.99},
			}},
			want:    "INSERT INTO products (name, price) VALUES (@p1, @p2), (@p3, @p4)",
			want1:   []any{"Widget", 9.99, "Gadget", 19.99},
			wantErr: false,
		},
		{
			name:    "record with nil table (error)",
			args:    args{record: []sql.Record{&mockNoTableRecord{}}},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseInsertQuery(tt.args.record...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseInsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseInsertQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseInsertQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
