package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestGetPlaceHolders(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  string
	}{
		{
			name:  "zero count",
			count: 0,
			want:  "",
		},
		{
			name:  "negative count",
			count: -1,
			want:  "",
		},
		{
			name:  "single placeholder",
			count: 1,
			want:  "?",
		},
		{
			name:  "multiple placeholders",
			count: 3,
			want:  "?, ?, ?",
		},
		{
			name:  "many placeholders",
			count: 5,
			want:  "?, ?, ?, ?, ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getPlaceHolders(tt.count)
			if got != tt.want {
				t.Errorf("getPlaceHolders() = %v, want %v", got, tt.want)
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
			record: &records.User{Id: 1, Name: "Alice", Email: "alice@example.com"},
			want:   "name, email, password_hash, score, is_active, created_at, updated_at",
		},
		{
			name:   "id only record",
			record: &mockIdOnlyRecord{Id: 1},
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
			record: []sql.Record{&records.User{Id: 1, Name: "Alice", Email: "alice@example.com"}},
			want:   "(?, ?, ?, ?, ?, ?, ?)",
			want1:  []any{"Alice", "alice@example.com", "", 0, 0, int64(0), int64(0)},
		},
		{
			name: "multiple records",
			record: []sql.Record{
				&records.User{Id: 1, Name: "Alice", Email: "alice@example.com"},
				&records.User{Id: 2, Name: "Bob", Email: "bob@example.com"},
			},
			want:  "(?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)",
			want1: []any{"Alice", "alice@example.com", "", 0, 0, int64(0), int64(0), "Bob", "bob@example.com", "", 0, 0, int64(0), int64(0)},
		},
		{
			name:   "id only record",
			record: []sql.Record{&mockIdOnlyRecord{Id: 1}},
			want:   "()",
			want1:  []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getValuesPlaceHolders(tt.record...)
			if got != tt.want {
				t.Errorf("getValuesPlaceHolders() got = %v, want %v", got, tt.want)
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
			name:   "simple fields",
			fields: []*sql.Field{sql.NewField("name"), sql.NewField("email")},
			want:   "name, email",
		},
		{
			name:   "fields with aliases",
			fields: []*sql.Field{sql.NewField("name").As("user_name"), sql.NewField("email").As("user_email")},
			want:   "name AS user_name, email AS user_email",
		},
		{
			name:   "fields with aggregate functions",
			fields: []*sql.Field{sql.CountOf(sql.NewField("id")), sql.SumOf(sql.NewField("amount"))},
			want:   "COUNT(id), SUM(amount)",
		},
		{
			name:   "fields with distinct",
			fields: []*sql.Field{sql.DistinctOf(sql.NewField("category"))},
			want:   "DISTINCT category",
		},
		{
			name:   "complex field with function and alias",
			fields: []*sql.Field{sql.CountOf(sql.NewField("id")).As("total_count")},
			want:   "COUNT(id) AS total_count",
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
			field: sql.DistinctOf(sql.NewField("category")),
			want:  "DISTINCT category",
		},
		{
			name:  "field with count function",
			field: sql.CountOf(sql.NewField("id")),
			want:  "COUNT(id)",
		},
		{
			name:  "field with sum function",
			field: sql.SumOf(sql.NewField("amount")),
			want:  "SUM(amount)",
		},
		{
			name:  "field with avg function",
			field: sql.AvgOf(sql.NewField("score")),
			want:  "AVG(score)",
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
			field: sql.CountOf(sql.NewField("id")).As("total"),
			want:  "COUNT(id) AS total",
		},
		{
			name:  "field with distinct and function",
			field: sql.CountOf(sql.DistinctOf(sql.NewField("category"))),
			want:  "COUNT(DISTINCT category)",
		},
		{
			name:  "nested field with function",
			field: sql.CountOf(sql.NewField("id")),
			want:  "COUNT(id)",
		},
		{
			name:  "empty field",
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
