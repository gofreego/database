package sql

import (
	"context"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type Join struct {
	Table *Table
	On    *Condition
}

type Table struct {
	Name string
	Join []Join
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
	SetID(id int64)
	Table() *Table
	Columns() []string
	Values() []any
	Scan(row Row) error
}

type Records interface {
	Table() *Table
	ScanMany(rows Rows) error
}

type Order int

const (
	Asc Order = iota
	Desc
)

type SortField struct {
	Field string
	Order Order
}

type Sort struct {
	fields []SortField
}

func NewSort() *Sort {
	return &Sort{
		fields: make([]SortField, 0),
	}
}

func (o *Sort) Add(field string, order Order) *Sort {
	o.fields = append(o.fields, SortField{Field: field, Order: order})
	return o
}

func (o *Sort) Fields() []SortField {
	return o.fields
}

type Operator int

const (
	Equal Operator = iota
	NotEqual
	GreaterThan
	GreaterThanOrEqual
	LessThan
	LessThanOrEqual
	IN
	NOTIN
	LIKE
	NOTLIKE
	ISNULL
	NOTNULL
	AND
	OR
	NOT
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
	Field      *string
	Value      any
	Operator   Operator
	Conditions []Condition
}

type Filter struct {
	Condition Condition
	GroupBy   *GroupBy
	Sort      *Sort
	Limit     int
	Offset    int
}

type Options struct {
	// if you want to use the primary database, use this option
	UsePrimaryDB bool
	// if you want to prepare the query, use this option
	PreparedName string
}

type UpdateField struct {
	Field string
	Value any
}

type Updates struct {
	Fields []UpdateField
}

func NewUpdates() *Updates {
	return &Updates{
		Fields: make([]UpdateField, 0),
	}
}

func (u *Updates) Add(field string, value any) *Updates {
	u.Fields = append(u.Fields, UpdateField{Field: field, Value: value})
	return u
}

type SQLDatabase interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	Insert(ctx context.Context, record Record, options ...Options) error
	InsertMany(ctx context.Context, records []Record, options ...Options) (int64, error)

	GetByID(ctx context.Context, record Record, options ...Options) error
	GetByFilter(ctx context.Context, filter Filter, record Record, options ...Options) error
	// This will update the record with the id of the record
	Update(ctx context.Context, record Record, options ...Options) error
	// This will update the records with the id of the records
	UpdateMany(ctx context.Context, records []Record, options ...Options) error
	// This will update the record with condition
	UpdateByCondition(ctx context.Context, condition *Condition, updates *Updates, options ...Options) error
	// This will delete the record with the id of the record
	DeleteByID(ctx context.Context, id int64, options ...Options) error
	// This will delete the record with condition
	DeleteByCondition(ctx context.Context, condition *Condition, options ...Options) error
}

func GetOptions(options ...Options) Options {
	if len(options) > 0 {
		return options[0]
	}
	return Options{}
}
