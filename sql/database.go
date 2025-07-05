package sql

import (
	"context"
)

type SQLDatabase interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	Insert(ctx context.Context, record Record, options ...Options) error
	InsertMany(ctx context.Context, records []Record, options ...Options) (int64, error)
	Upsert(ctx context.Context, record Record, options ...Options) error
	GetByID(ctx context.Context, record Record, options ...Options) error
	GetByFilter(ctx context.Context, filter *Filter, values []any, record Records, options ...Options) error
	// This will update the record with the id of the record
	UpdateByID(ctx context.Context, record Record, options ...Options) error
	// This will update the records with the id of the records
	UpdateMany(ctx context.Context, records []Record, options ...Options) error
	// This will update the record with condition
	UpdateByCondition(ctx context.Context, updates *Updates, condition *Condition, values []any, options ...Options) error
	// This will delete the record with the id of the record
	DeleteByID(ctx context.Context, id int64, options ...Options) error
	// This will delete the record with condition
	DeleteByCondition(ctx context.Context, condition *Condition, values []any, options ...Options) error
}

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type Join struct {
	Type  JoinType
	Table *Table
	On    *Condition
}

type Table struct {
	Name  string
	Alias string
	Join  []Join
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
		Join: make([]Join, 0),
	}
}

func (t *Table) WithInnerJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
	})
	return t
}

func (t *Table) WithLeftJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
	})
	return t
}

func (t *Table) WithRightJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
	})
	return t
}

type Row interface {
	Scan(dest ...any) error
}

type Rows interface {
	Row
	Next() bool
}

// record fields should be exported and should have a sql tag for the column name
type Record interface {
	ID() int64
	IdColumn() string
	SetID(id int64)
	Table() *Table
	Columns() []string
	Values() []any
	Scan(row Row) error
}

type Records interface {
	Table() *Table
	Columns() []string
	ScanMany(rows Rows) error
}

type Order int

const (
	Asc Order = iota
	Desc
)

type OrderBy struct {
	Field string
	Order Order
}

type Sort struct {
	fields []OrderBy
}

func NewSort() *Sort {
	return &Sort{
		fields: make([]OrderBy, 0),
	}
}

func (o *Sort) Add(field string, order Order) *Sort {
	o.fields = append(o.fields, OrderBy{Field: field, Order: order})
	return o
}

func (o *Sort) Fields() []OrderBy {
	return o.fields
}

type Operator int

const (
	// Comparison Operators
	EQ  Operator = iota // Equal to
	NEQ                 // Not equal to
	GT                  // Greater than
	GTE                 // Greater than or equal to
	LT                  // Less than
	LTE                 // Less than or equal to
	// Logical Operators
	AND // Both conditions must be true
	OR  // At least one condition is true
	NOT // Negates the condition
	// Special Operators
	IN         // Value exists in a list
	NOTIN      // Value does not exist in list
	LIKE       // Pattern match (wildcard %, _)
	NOTLIKE    // Pattern not matching\
	ISNULL     // Field is NULL
	ISNOTNULL  // Field is not NULL
	EXISTS     // Subquery returns rows
	NOTEXISTS  // Subquery returns no rows
	REGEXP     // Matches regular expression
	BETWEEN    // Value is within range (inclusive)
	NOTBETWEEN // Value is not within range (inclusive)
)

type GroupBy struct {
	fields []string
}

func NewGroupBy(fields ...string) *GroupBy {
	return &GroupBy{
		fields: fields,
	}
}

func (g *GroupBy) Fields() []string {
	return g.fields
}

type Condition struct {
	Field       string
	ValueIndex  int
	ValuesCount int
	Operator    Operator
	Conditions  []Condition
}

type Filter struct {
	Condition *Condition
	GroupBy   *GroupBy
	Sort      *Sort
	Limit     int
	Offset    int
}

type Options struct {
	// if you want to use the primary database, use this option
	UsePrimaryDB bool
	// if you want to prepare the query, use this option
	PreparedName string // It should be unique for each diff type of query
}

type UpdateField struct {
	Field      string
	ValueIndex int
}

type Updates struct {
	Fields []UpdateField
}

func NewUpdates() *Updates {
	return &Updates{
		Fields: make([]UpdateField, 0),
	}
}

func (u *Updates) Add(field string, valueIndex int) *Updates {
	u.Fields = append(u.Fields, UpdateField{Field: field, ValueIndex: valueIndex})
	return u
}

func GetOptions(options ...Options) Options {
	if len(options) > 0 {
		return options[0]
	}
	return Options{}
}

type ValueType int

const (
	Any ValueType = iota
	String
	Array
	Bool
)

type Value struct {
	Type   ValueType
	Index  int
	Length int
}

func AnyValue(index int) *Value {
	return &Value{
		Type:  Any,
		Index: index,
	}
}

func StringValue(index int) *Value {
	return &Value{
		Type:  String,
		Index: index,
	}
}

func ArrayValue(index int, length int) *Value {
	return &Value{
		Type:  Array,
		Index: index,
	}
}

func BoolValue(index int) *Value {
	return &Value{
		Type:  Bool,
		Index: index,
	}
}
