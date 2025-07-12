package sql

import (
	"context"
	"reflect"
)

type Database interface {
	// Ping the database to check if the connection is alive
	Ping(ctx context.Context) error
	// Close the database connection and prepared statements
	Close(ctx context.Context) error
	// Insert a single record into the database
	Insert(ctx context.Context, record Record, options ...Options) error
	// Insert multiple records into the database
	InsertMany(ctx context.Context, records []Record, options ...Options) (int64, error)
	// Upsert a single record into the database
	Upsert(ctx context.Context, record Record, options ...Options) (bool, error)
	// Get a single record by id and scan the record values into the record
	GetByID(ctx context.Context, record Record, options ...Options) error
	// Get multiple records by filter and scan the records into the record
	Get(ctx context.Context, filter *Filter, values []any, record Records, options ...Options) error
	// This will update the record with the id of the record and return if the record is updated
	UpdateByID(ctx context.Context, record Record, options ...Options) (bool, error)
	// This will update the records with condition and return the number of rows affected
	Update(ctx context.Context, table *Table, updates *Updates, condition *Condition, values []any, options ...Options) (int64, error)
	// This will soft delete the record with the id of the record and return if the record is soft deleted
	SoftDelete(ctx context.Context, record Record, options ...Options) (bool, error)
	// This will delete the record with the id of the record and return if the record is deleted
	DeleteByID(ctx context.Context, record Record, options ...Options) (bool, error)
	// This will delete the records with condition and return the number of rows affected
	Delete(ctx context.Context, table *Table, condition *Condition, values []any, options ...Options) (int64, error)
}

/*
*
************ Table
************ Table is used to represent a table in the database.
*
*
 */

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
		Type:  InnerJoin,
	})
	return t
}

func (t *Table) WithLeftJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
		Type:  LeftJoin,
	})
	return t
}

func (t *Table) WithRightJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
		Type:  RightJoin,
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
	// This is useful to remove id column while inserting the records
	IdColumn() string
	SetID(id int64)
	Table() *Table
	Columns() []string
	Values() []any
	Scan(row Row) error
	SetDeleted(deleted bool)
}

type Records interface {
	Table() *Table
	Columns() []string
	Scan(rows Rows) error
}

/************ Sorting ************/

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

/************ Conditions ************/

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

func (o Operator) String() string {
	switch o {
	case EQ:
		return "EQ"
	case NEQ:
		return "NEQ"
	case GT:
		return "GT"
	case GTE:
		return "GTE"
	case LT:
		return "LT"
	case LTE:
		return "LTE"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case NOT:
		return "NOT"
	case IN:
		return "IN"
	case NOTIN:
		return "NOT IN"
	case LIKE:
		return "LIKE"
	case NOTLIKE:
		return "NOT LIKE"
	case ISNULL:
		return "IS NULL"
	case ISNOTNULL:
		return "IS NOT NULL"
	case EXISTS:
		return "EXISTS"
	case NOTEXISTS:
		return "NOT EXISTS"
	case REGEXP:
		return "REGEXP"
	case BETWEEN:
		return "BETWEEN"
	case NOTBETWEEN:
		return "NOT BETWEEN"
	default:
		return ""
	}
}

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
	Field      string // The field name to apply the condition on i.e. column name (e. "email", "is_active")
	Value      *Value
	Operator   Operator    // The operator to apply the condition (e.g. EQ, NEQ, GT, etc.)
	Conditions []Condition // Nested conditions for AND/OR operations
}

func (c *Condition) Validate() error {
	if c == nil {
		return nil // No condition to validate
	}
	switch c.Operator {
	case AND, OR, NOT:
		if c.Field != "" || c.Value != nil {
			return NewInvalidQueryError("invalid condition: value should be nil and field should be empty for AND/OR/NOT operator,for filed: %s, Operator:%s", c.Field, c.Operator.String())
		}
		if len(c.Conditions) == 0 {
			return NewInvalidQueryError("invalid condition: conditions should not be empty for AND/OR/NOT operator")
		}
		return nil
	case EQ, NEQ, GT, GTE, LT, LTE, IN, NOTIN, LIKE, NOTLIKE, REGEXP, BETWEEN, NOTBETWEEN:
		if c.Field == "" {
			return NewInvalidQueryError("invalid condition: field should not be empty for operator %s", c.Operator.String())
		}
		if c.Value == nil {
			return NewInvalidQueryError("invalid condition: value should not be nil for operator %s", c.Operator.String())
		}
		if len(c.Conditions) > 0 {
			return NewInvalidQueryError("invalid condition: conditions should be empty for operator %s, for field: %s", c.Operator.String(), c.Field)
		}
		return nil
	case ISNULL, ISNOTNULL:
		if c.Field == "" {
			return NewInvalidQueryError("invalid condition: field should not be empty for operator %s", c.Operator.String())
		}
		if c.Value != nil {
			return NewInvalidQueryError("invalid condition: value should be nil for operator %s, for field: %s", c.Operator.String(), c.Field)
		}
		if len(c.Conditions) > 0 {
			return NewInvalidQueryError("invalid condition: conditions should be empty for operator %s, for field: %s", c.Operator.String(), c.Field)
		}
		return nil
	case EXISTS, NOTEXISTS:
		return NewInvalidQueryError("invalid condition: EXISTS and NOT EXISTS operators not supported, field: %s, Operator: %s", c.Field, c.Operator.String())
	default:
		return NewInvalidQueryError("invalid condition: unknown operator %s, for field: %s", c.Operator.String(), c.Field)
	}
}

/*
*
*
************ Filter
************ Filter is used to filter the records based on the condition, group by, sort and
*
*
 */

type Filter struct {
	Condition *Condition // The main condition for the filter
	GroupBy   *GroupBy   // Grouping fields for aggregation
	Sort      *Sort      // Sorting fields for the result set
	Limit     *Value     // Limit the number of records returned
	Offset    *Value     // Offset the number of records returned
}

/*
**
************ Updates
************ Updates are used to update the fields of a record.
*
*
 */

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

/*
*
*
 ***** Joins
*
*
*/

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

/*
*
*
************ Values
************ These are used for internal purposes to represent the type of value for adding validations.
*
*
 */
type ValueType int

const (
	Any ValueType = iota
	Column
)

type Value struct {
	// This is used for internal purpose to specify the type of value needed for validation
	Type ValueType
	// // This is used for fixed values that are not parameterized. These values will be hardcoded
	// // in the query. Use this carefully, as it may lead to SQL injection if the value is not sanitized.
	Value any
	// This is used for parameterized queries where the value is not fixed and is passed as a parameter.
	// if value is used for IN/NOTIN operators. use WithCount to specify the number of values to use from the values slice
	// Index is the index of the value in the values slice for parameterized queries
	Index int
	Count int // This is used for IN/NOTIN operators to specify the number of values to use from the values slice
}

// This method is used for internal purpose
func (v *Value) WithType(t ValueType) *Value {
	v.Type = t
	return v
}

func (v *Value) WithCount(count int) *Value {
	v.Count = count
	return v
}

func (v *Value) IsColumn() bool {
	return v.Type == Column
}

func (v *Value) IsStringValue() bool {
	if v.Value == nil {
		return false
	}
	return reflect.TypeOf(v.Value).Kind() == reflect.String
}

func (v *Value) IsValue() bool {
	return v.Value != nil
}

// NewIndexedValue creates a new Value with the specified index.
// This is used for parameterized queries where the value is not fixed and is passed as a parameter.
// if value is used for IN/NOTIN operators. use WithCount to specify the number of values to use from the values slice
func NewIndexedValue(index int) *Value {
	return &Value{
		Type:  Any,
		Index: index,
	}
}

// NewValue creates a new Value with the specified value.
// This is used for fixed values that are not parameterized. These values will be hardcoded in the query.
// Use this carefully, as it may lead to SQL injection if the value is not sanitized.
func NewValue(value any) *Value {
	return &Value{
		Type:  Any,
		Value: value,
	}
}

func NewColumnValue(column string) *Value {
	return &Value{
		Type:  Column,
		Value: column,
	}
}

// Asssuming validation is already applied that valuesPassed has enough values
func GetValues(indexs []int, values []any) []any {
	if len(indexs) == 0 {
		return nil
	}
	result := make([]any, 0)
	for _, v := range indexs {
		value := values[v]
		// check if value is array type
		if reflect.TypeOf(value).Kind() == reflect.Slice || reflect.TypeOf(value).Kind() == reflect.Array {
			// if it is array type, append all the values to the result
			sliceValue := reflect.ValueOf(value)
			for i := 0; i < sliceValue.Len(); i++ {
				result = append(result, sliceValue.Index(i).Interface())
			}
		} else {
			// if it is not array type, append the value directly
			result = append(result, value)
		}
	}
	return result
}

/*
 # Options is used to pass additional options to the database operations.
 # It can be used to specify whether to use the primary database or to prepare the query.
*/

type Options struct {
	// if you want to use the primary database, use this option
	// no need to set this in case of write operations. It by default chooses primary db.
	UsePrimaryDB bool
	// if you want to prepare the query, use this option
	PreparedName string // It should be unique for each diff type of query
	Timeout      int64  // Timeout in milliseconds for the query execution
}

// GetOptions returns the first option from the options slice if available, otherwise returns an empty Options struct.
// This is used internal purpose of the library
func GetOptions(options ...Options) Options {
	if len(options) > 0 {
		return options[0]
	}
	return Options{}
}
