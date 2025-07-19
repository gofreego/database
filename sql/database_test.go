package sql

import (
	"reflect"
	"testing"
)

func TestGetValues(t *testing.T) {
	type args struct {
		indexs []int
		values []any
	}
	tests := []struct {
		name string
		args args
		want []any
	}{
		{
			name: "basic values",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{1, 2, 3},
			},
			want: []any{1, 2, 3},
		},
		{
			name: "empty indexs",
			args: args{
				indexs: []int{},
				values: []any{1, 2, 3},
			},
			want: nil,
		},
		{
			name: "single value",
			args: args{
				indexs: []int{0},
				values: []any{"test"},
			},
			want: []any{"test"},
		},
		{
			name: "mixed types",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{"string", 42, true},
			},
			want: []any{"string", 42, true},
		},
		{
			name: "with array values",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{[]int{1, 2, 3}, "string", []string{"a", "b"}},
			},
			want: []any{1, 2, 3, "string", "a", "b"},
		},
		{
			name: "nested arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]int{1, 2}, []int{3, 4}},
			},
			want: []any{1, 2, 3, 4},
		},
		{
			name: "string arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]string{"hello", "world"}, []string{"test"}},
			},
			want: []any{"hello", "world", "test"},
		},
		{
			name: "mixed arrays and scalars",
			args: args{
				indexs: []int{0, 1, 2},
				values: []any{[]int{1, 2}, "middle", []string{"end"}},
			},
			want: []any{1, 2, "middle", "end"},
		},
		{
			name: "empty arrays",
			args: args{
				indexs: []int{0, 1},
				values: []any{[]int{}, []string{}},
			},
			want: []any{},
		},
		{
			name: "complex nested structure",
			args: args{
				indexs: []int{0, 1, 2, 3},
				values: []any{
					[]int{1, 2},
					"string",
					[]string{"a", "b", "c"},
					[]bool{true},
				},
			},
			want: []any{1, 2, "string", "a", "b", "c", true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetValues(tt.args.indexs, tt.args.values); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewTable(t *testing.T) {
	tests := []struct {
		name string
		want *Table
	}{
		{
			name: "create new table",
			want: &Table{
				Name:  "users",
				Alias: "",
				Join:  make([]Join, 0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTable("users"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTable_WithInnerJoin(t *testing.T) {
	table := NewTable("users")
	joinTable := NewTable("profiles")
	condition := &Condition{
		Field:    "users.id",
		Operator: EQ,
		Value:    NewColumnValue("profiles.user_id"),
	}

	result := table.WithInnerJoin(joinTable, condition)

	if len(result.Join) != 1 {
		t.Errorf("Expected 1 join, got %d", len(result.Join))
	}

	join := result.Join[0]
	if join.Type != InnerJoin {
		t.Errorf("Expected InnerJoin, got %v", join.Type)
	}
	if join.Table != joinTable {
		t.Errorf("Expected join table to match")
	}
	if join.On != condition {
		t.Errorf("Expected join condition to match")
	}
}

func TestTable_WithLeftJoin(t *testing.T) {
	table := NewTable("users")
	joinTable := NewTable("profiles")
	condition := &Condition{
		Field:    "users.id",
		Operator: EQ,
		Value:    NewColumnValue("profiles.user_id"),
	}

	result := table.WithLeftJoin(joinTable, condition)

	if len(result.Join) != 1 {
		t.Errorf("Expected 1 join, got %d", len(result.Join))
	}

	join := result.Join[0]
	if join.Type != LeftJoin {
		t.Errorf("Expected LeftJoin, got %v", join.Type)
	}
	if join.Table != joinTable {
		t.Errorf("Expected join table to match")
	}
	if join.On != condition {
		t.Errorf("Expected join condition to match")
	}
}

func TestTable_WithRightJoin(t *testing.T) {
	table := NewTable("users")
	joinTable := NewTable("profiles")
	condition := &Condition{
		Field:    "users.id",
		Operator: EQ,
		Value:    NewColumnValue("profiles.user_id"),
	}

	result := table.WithRightJoin(joinTable, condition)

	if len(result.Join) != 1 {
		t.Errorf("Expected 1 join, got %d", len(result.Join))
	}

	join := result.Join[0]
	if join.Type != RightJoin {
		t.Errorf("Expected RightJoin, got %v", join.Type)
	}
	if join.Table != joinTable {
		t.Errorf("Expected join table to match")
	}
	if join.On != condition {
		t.Errorf("Expected join condition to match")
	}
}

func TestNewSort(t *testing.T) {
	sort := NewSort()
	if sort == nil {
		t.Errorf("NewSort() returned nil")
	}
	if len(sort.fields) != 0 {
		t.Errorf("Expected empty fields slice, got %d", len(sort.fields))
	}
}

func TestSort_Add(t *testing.T) {
	sort := NewSort()

	// Test adding a single field
	result := sort.Add("name", Asc)
	if result != sort {
		t.Errorf("Expected method chaining to return same instance")
	}

	if len(sort.fields) != 1 {
		t.Errorf("Expected 1 field, got %d", len(sort.fields))
	}

	field := sort.fields[0]
	if field.Field != "name" {
		t.Errorf("Expected field name 'name', got %s", field.Field)
	}
	if field.Order != Asc {
		t.Errorf("Expected order Asc, got %v", field.Order)
	}

	// Test adding multiple fields
	sort.Add("created_at", Desc)
	if len(sort.fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(sort.fields))
	}
}

func TestSort_Fields(t *testing.T) {
	sort := NewSort()
	sort.Add("name", Asc)
	sort.Add("created_at", Desc)

	fields := sort.Fields()
	if len(fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(fields))
	}

	if fields[0].Field != "name" || fields[0].Order != Asc {
		t.Errorf("First field mismatch")
	}
	if fields[1].Field != "created_at" || fields[1].Order != Desc {
		t.Errorf("Second field mismatch")
	}
}

func TestOperator_String(t *testing.T) {
	tests := []struct {
		name     string
		operator Operator
		want     string
	}{
		{"EQ", EQ, "EQ"},
		{"NEQ", NEQ, "NEQ"},
		{"GT", GT, "GT"},
		{"GTE", GTE, "GTE"},
		{"LT", LT, "LT"},
		{"LTE", LTE, "LTE"},
		{"AND", AND, "AND"},
		{"OR", OR, "OR"},
		{"NOT", NOT, "NOT"},
		{"IN", IN, "IN"},
		{"NOTIN", NOTIN, "NOT IN"},
		{"LIKE", LIKE, "LIKE"},
		{"NOTLIKE", NOTLIKE, "NOT LIKE"},
		{"ISNULL", ISNULL, "IS NULL"},
		{"ISNOTNULL", ISNOTNULL, "IS NOT NULL"},
		{"EXISTS", EXISTS, "EXISTS"},
		{"NOTEXISTS", NOTEXISTS, "NOT EXISTS"},
		{"REGEXP", REGEXP, "REGEXP"},
		{"BETWEEN", BETWEEN, "BETWEEN"},
		{"NOTBETWEEN", NOTBETWEEN, "NOT BETWEEN"},
		{"unknown", 999, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.operator.String(); got != tt.want {
				t.Errorf("Operator.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewGroupBy(t *testing.T) {
	tests := []struct {
		name   string
		fields []string
		want   *GroupBy
	}{
		{
			name:   "single field",
			fields: []string{"department"},
			want:   &GroupBy{fields: []string{"department"}},
		},
		{
			name:   "multiple fields",
			fields: []string{"department", "role"},
			want:   &GroupBy{fields: []string{"department", "role"}},
		},
		{
			name:   "no fields",
			fields: []string{},
			want:   &GroupBy{fields: []string{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewGroupBy(tt.fields...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewGroupBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupBy_Fields(t *testing.T) {
	groupBy := NewGroupBy("department", "role")
	fields := groupBy.Fields()

	expected := []string{"department", "role"}
	if !reflect.DeepEqual(fields, expected) {
		t.Errorf("GroupBy.Fields() = %v, want %v", fields, expected)
	}
}

func TestCondition_Validate(t *testing.T) {
	tests := []struct {
		name      string
		condition *Condition
		wantErr   bool
	}{
		{
			name:      "nil condition",
			condition: nil,
			wantErr:   false,
		},
		{
			name: "valid AND condition",
			condition: &Condition{
				Operator: AND,
				Conditions: []Condition{
					{Field: "active", Operator: EQ, Value: NewValue(true)},
					{Field: "role", Operator: EQ, Value: NewValue("admin")},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid AND condition - has field",
			condition: &Condition{
				Field:    "active",
				Operator: AND,
				Conditions: []Condition{
					{Field: "active", Operator: EQ, Value: NewValue(true)},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid AND condition - has value",
			condition: &Condition{
				Value:    NewValue(true),
				Operator: AND,
				Conditions: []Condition{
					{Field: "active", Operator: EQ, Value: NewValue(true)},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid AND condition - empty conditions",
			condition: &Condition{
				Operator:   AND,
				Conditions: []Condition{},
			},
			wantErr: true,
		},
		{
			name: "valid EQ condition",
			condition: &Condition{
				Field:    "email",
				Operator: EQ,
				Value:    NewValue("test@example.com"),
			},
			wantErr: false,
		},
		{
			name: "invalid EQ condition - empty field",
			condition: &Condition{
				Operator: EQ,
				Value:    NewValue("test@example.com"),
			},
			wantErr: true,
		},
		{
			name: "invalid EQ condition - nil value",
			condition: &Condition{
				Field:    "email",
				Operator: EQ,
			},
			wantErr: true,
		},
		{
			name: "invalid EQ condition - has nested conditions",
			condition: &Condition{
				Field:    "email",
				Operator: EQ,
				Value:    NewValue("test@example.com"),
				Conditions: []Condition{
					{Field: "active", Operator: EQ, Value: NewValue(true)},
				},
			},
			wantErr: true,
		},
		{
			name: "valid ISNULL condition",
			condition: &Condition{
				Field:    "deleted_at",
				Operator: ISNULL,
			},
			wantErr: false,
		},
		{
			name: "invalid ISNULL condition - empty field",
			condition: &Condition{
				Operator: ISNULL,
			},
			wantErr: true,
		},
		{
			name: "invalid ISNULL condition - has value",
			condition: &Condition{
				Field:    "deleted_at",
				Operator: ISNULL,
				Value:    NewValue("not null"),
			},
			wantErr: true,
		},
		{
			name: "invalid ISNULL condition - has nested conditions",
			condition: &Condition{
				Field:    "deleted_at",
				Operator: ISNULL,
				Conditions: []Condition{
					{Field: "active", Operator: EQ, Value: NewValue(true)},
				},
			},
			wantErr: true,
		},
		{
			name: "EXISTS operator not supported",
			condition: &Condition{
				Field:    "id",
				Operator: EXISTS,
			},
			wantErr: true,
		},
		{
			name: "NOTEXISTS operator not supported",
			condition: &Condition{
				Field:    "id",
				Operator: NOTEXISTS,
			},
			wantErr: true,
		},
		{
			name: "unknown operator",
			condition: &Condition{
				Field:    "id",
				Operator: 999,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.condition.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Condition.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewUpdates(t *testing.T) {
	updates := NewUpdates()
	if updates == nil {
		t.Errorf("NewUpdates() returned nil")
	}
	if len(updates.Fields) != 0 {
		t.Errorf("Expected empty fields slice, got %d", len(updates.Fields))
	}
}

func TestUpdates_Add(t *testing.T) {
	updates := NewUpdates()

	// Test adding a single field
	result := updates.Add("name", NewValue("John"))
	if result != updates {
		t.Errorf("Expected method chaining to return same instance")
	}

	if len(updates.Fields) != 1 {
		t.Errorf("Expected 1 field, got %d", len(updates.Fields))
	}

	field := updates.Fields[0]
	if field.Field != "name" {
		t.Errorf("Expected field name 'name', got %s", field.Field)
	}
	if field.Value.Value != "John" {
		t.Errorf("Expected value 'John', got %v", field.Value.Value)
	}

	// Test adding multiple fields
	updates.Add("email", NewValue("john@example.com"))
	if len(updates.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(updates.Fields))
	}
}

func TestValue_WithType(t *testing.T) {
	value := &Value{}
	result := value.WithType(Column)

	if result != value {
		t.Errorf("Expected method chaining to return same instance")
	}
	if value.Type != Column {
		t.Errorf("Expected type Column, got %v", value.Type)
	}
}

func TestValue_WithCount(t *testing.T) {
	value := &Value{}
	result := value.WithCount(5)

	if result != value {
		t.Errorf("Expected method chaining to return same instance")
	}
	if value.Count != 5 {
		t.Errorf("Expected count 5, got %d", value.Count)
	}
}

func TestValue_IsColumn(t *testing.T) {
	tests := []struct {
		name  string
		value *Value
		want  bool
	}{
		{
			name:  "column type",
			value: &Value{Type: Column},
			want:  true,
		},
		{
			name:  "any type",
			value: &Value{Type: Any},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.IsColumn(); got != tt.want {
				t.Errorf("Value.IsColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValue_IsStringValue(t *testing.T) {
	tests := []struct {
		name  string
		value *Value
		want  bool
	}{
		{
			name:  "string value",
			value: &Value{Value: "test"},
			want:  true,
		},
		{
			name:  "int value",
			value: &Value{Value: 42},
			want:  false,
		},
		{
			name:  "nil value",
			value: &Value{Value: nil},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.IsStringValue(); got != tt.want {
				t.Errorf("Value.IsStringValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValue_IsValue(t *testing.T) {
	tests := []struct {
		name  string
		value *Value
		want  bool
	}{
		{
			name:  "has value",
			value: &Value{Value: "test"},
			want:  true,
		},
		{
			name:  "nil value",
			value: &Value{Value: nil},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.value.IsValue(); got != tt.want {
				t.Errorf("Value.IsValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewIndexedValue(t *testing.T) {
	value := NewIndexedValue(5)

	if value.Type != Any {
		t.Errorf("Expected type Any, got %v", value.Type)
	}
	if value.Index != 5 {
		t.Errorf("Expected index 5, got %d", value.Index)
	}
	if value.Value != nil {
		t.Errorf("Expected nil value, got %v", value.Value)
	}
}

func TestNewValue(t *testing.T) {
	value := NewValue("test")

	if value.Type != Any {
		t.Errorf("Expected type Any, got %v", value.Type)
	}
	if value.Value != "test" {
		t.Errorf("Expected value 'test', got %v", value.Value)
	}
	if value.Index != 0 {
		t.Errorf("Expected index 0, got %d", value.Index)
	}
}

func TestNewColumnValue(t *testing.T) {
	value := NewColumnValue("users.id")

	if value.Type != Column {
		t.Errorf("Expected type Column, got %v", value.Type)
	}
	if value.Value != "users.id" {
		t.Errorf("Expected value 'users.id', got %v", value.Value)
	}
	if value.Index != 0 {
		t.Errorf("Expected index 0, got %d", value.Index)
	}
}

func TestGetOptions(t *testing.T) {
	tests := []struct {
		name    string
		options []Options
		want    Options
	}{
		{
			name:    "no options",
			options: []Options{},
			want:    Options{},
		},
		{
			name: "single option",
			options: []Options{
				{UsePrimaryDB: true, PreparedName: "test", Timeout: 5000},
			},
			want: Options{UsePrimaryDB: true, PreparedName: "test", Timeout: 5000},
		},
		{
			name: "multiple options - returns first",
			options: []Options{
				{UsePrimaryDB: true, PreparedName: "first", Timeout: 5000},
				{UsePrimaryDB: false, PreparedName: "second", Timeout: 10000},
			},
			want: Options{UsePrimaryDB: true, PreparedName: "first", Timeout: 5000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetOptions(tt.options...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}
