package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
)

func TestGetPlaceHolders(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		lastIndex int
		want      string
		wantIndex int
	}{
		{
			name:      "zero count",
			count:     0,
			lastIndex: 0,
			want:      "",
			wantIndex: 0,
		},
		{
			name:      "negative count",
			count:     -1,
			lastIndex: 0,
			want:      "",
			wantIndex: 0,
		},
		{
			name:      "single placeholder",
			count:     1,
			lastIndex: 0,
			want:      "$1",
			wantIndex: 1,
		},
		{
			name:      "multiple placeholders",
			count:     3,
			lastIndex: 0,
			want:      "$1, $2, $3",
			wantIndex: 3,
		},
		{
			name:      "many placeholders",
			count:     5,
			lastIndex: 10,
			want:      "$11, $12, $13, $14, $15",
			wantIndex: 15,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastIndex := tt.lastIndex
			got := getPlaceHolders(tt.count, &lastIndex)
			if got != tt.want {
				t.Errorf("getPlaceHolders() = %v, want %v", got, tt.want)
			}
			if lastIndex != tt.wantIndex {
				t.Errorf("getPlaceHolders() lastIndex = %v, want %v", lastIndex, tt.wantIndex)
			}
		})
	}
}

func TestParseInsertColumns(t *testing.T) {
	tests := []struct {
		name   string
		record sql.Record
		want   string
	}{
		{
			name:   "user record with multiple columns",
			record: &mockUserRecord{},
			want:   "name, email, age",
		},
		{
			name:   "id only record",
			record: &mockIdOnlyRecord{},
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseInsertColumns(tt.record)
			if got != tt.want {
				t.Errorf("parseInsertColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetValuesPlaceHolders(t *testing.T) {
	tests := []struct {
		name   string
		record []sql.Record
		want   string
		want1  []any
	}{
		{
			name:   "single record",
			record: []sql.Record{&mockUserRecord{}},
			want:   "($1, $2, $3)",
			want1:  []any{"Alice", "alice@example.com", 25},
		},
		{
			name:   "multiple records",
			record: []sql.Record{&mockUserRecord{}, &mockUserRecord{}},
			want:   "($1, $2, $3), ($4, $5, $6)",
			want1:  []any{"Alice", "alice@example.com", 25, "Alice", "alice@example.com", 25},
		},
		{
			name:   "id only record",
			record: []sql.Record{&mockIdOnlyRecord{}},
			want:   "()",
			want1:  []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastIndex := 0
			got, got1 := getValuesPlaceHolders(&lastIndex, tt.record...)
			if got != tt.want {
				t.Errorf("getValuesPlaceHolders() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("getValuesPlaceHolders() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseColumns(t *testing.T) {
	tests := []struct {
		name   string
		fields []*sql.Field
		want   string
	}{
		{
			name: "simple fields",
			fields: []*sql.Field{
				sql.NewField("name"),
				sql.NewField("email"),
				sql.NewField("age"),
			},
			want: "name, email, age",
		},
		{
			name: "fields with aliases",
			fields: []*sql.Field{
				sql.NewField("name").As("user_name"),
				sql.NewField("email").As("user_email"),
			},
			want: "name AS user_name, email AS user_email",
		},
		{
			name: "fields with aggregate functions",
			fields: []*sql.Field{
				sql.CountOf(sql.NewField("id")),
				sql.AvgOf(sql.NewField("age")),
				sql.SumOf(sql.NewField("salary")),
			},
			want: "COUNT(id), AVG(age), SUM(salary)",
		},
		{
			name: "fields with distinct",
			fields: []*sql.Field{
				sql.DistinctOf(sql.NewField("name")),
				sql.DistinctOf(sql.NewField("email")),
			},
			want: "DISTINCT name, DISTINCT email",
		},
		{
			name: "complex field with function and alias",
			fields: []*sql.Field{
				sql.AvgOf(sql.NewField("age")).As("average_age"),
			},
			want: "AVG(age) AS average_age",
		},
		{
			name:   "empty fields",
			fields: []*sql.Field{},
			want:   "",
		},
		{
			name:   "nil fields",
			fields: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseColumns(tt.fields)
			if got != tt.want {
				t.Errorf("parseColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseField(t *testing.T) {
	tests := []struct {
		name  string
		field *sql.Field
		want  string
	}{
		{
			name:  "simple field",
			field: sql.NewField("name"),
			want:  "name",
		},
		{
			name:  "field with alias",
			field: sql.NewField("name").As("user_name"),
			want:  "name AS user_name",
		},
		{
			name:  "field with distinct",
			field: sql.DistinctOf(sql.NewField("name")),
			want:  "DISTINCT name",
		},
		{
			name:  "field with count function",
			field: sql.CountOf(sql.NewField("id")),
			want:  "COUNT(id)",
		},
		{
			name:  "field with sum function",
			field: sql.SumOf(sql.NewField("salary")),
			want:  "SUM(salary)",
		},
		{
			name:  "field with avg function",
			field: sql.AvgOf(sql.NewField("age")),
			want:  "AVG(age)",
		},
		{
			name:  "field with min function",
			field: sql.MinOf(sql.NewField("price")),
			want:  "MIN(price)",
		},
		{
			name:  "field with max function",
			field: sql.MaxOf(sql.NewField("price")),
			want:  "MAX(price)",
		},
		{
			name:  "field with function and alias",
			field: sql.AvgOf(sql.NewField("age")).As("average_age"),
			want:  "AVG(age) AS average_age",
		},
		{
			name:  "field with distinct and function",
			field: sql.CountOf(sql.DistinctOf(sql.NewField("name"))),
			want:  "COUNT(DISTINCT name)",
		},
		{
			name:  "nested field with function",
			field: sql.CountOf(sql.NewField("subfield")),
			want:  "COUNT(subfield)",
		},
		{
			name:  "empty field",
			field: sql.NewField(""),
			want:  "",
		},
		{
			name:  "nested empty field",
			field: &sql.Field{Field: &sql.Field{}},
			want:  "",
		},
		{
			name:  "nested field with distinct",
			field: &sql.Field{Field: sql.NewField("subfield"), Distinct: true},
			want:  "DISTINCT subfield",
		},
		{
			name:  "nested field with function",
			field: &sql.Field{Field: sql.NewField("subfield"), Func: sql.Count},
			want:  "COUNT(subfield)",
		},
		{
			name:  "nested field with alias",
			field: &sql.Field{Field: sql.NewField("subfield"), Alias: "sub"},
			want:  "subfield AS sub",
		},
		{
			name:  "nested field with distinct and function",
			field: &sql.Field{Field: sql.NewField("subfield"), Distinct: true, Func: sql.Count},
			want:  "COUNT(DISTINCT subfield)",
		},
		{
			name:  "nested field with function and alias",
			field: &sql.Field{Field: sql.NewField("subfield"), Func: sql.Avg, Alias: "average"},
			want:  "AVG(subfield) AS average",
		},
		{
			name:  "nested field with distinct, function and alias",
			field: &sql.Field{Field: sql.NewField("subfield"), Distinct: true, Func: sql.Sum, Alias: "total"},
			want:  "SUM(DISTINCT subfield) AS total",
		},
		{
			name:  "field with no name and no nested field",
			field: &sql.Field{},
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseField(tt.field)
			if got != tt.want {
				t.Errorf("parseField() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Mock records for testing
type mockUserRecord struct{}

func (m *mockUserRecord) ID() int64         { return 1 }
func (m *mockUserRecord) IdColumn() string  { return "id" }
func (m *mockUserRecord) SetID(id int64)    {}
func (m *mockUserRecord) Table() *sql.Table { return sql.NewTable("users") }
func (m *mockUserRecord) Columns() []*sql.Field {
	return []*sql.Field{sql.NewField("id"), sql.NewField("name"), sql.NewField("email"), sql.NewField("age")}
}
func (m *mockUserRecord) Values() []any           { return []any{"Alice", "alice@example.com", 25} }
func (m *mockUserRecord) Scan(row sql.Row) error  { return nil }
func (m *mockUserRecord) SetDeleted(deleted bool) {}
