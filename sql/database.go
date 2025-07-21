package sql

import (
	"context"
	"reflect"
)

// Database defines the interface for database operations.
// It provides a unified API for different database engines (PostgreSQL, MySQL, MSSQL).
type Database interface {
	// Ping checks if the database connection is alive.
	// Returns an error if the connection is not available.
	Ping(ctx context.Context) error

	// Close closes the database connection and cleans up prepared statements.
	// Should be called when the database is no longer needed.
	Close(ctx context.Context) error

	// Insert adds a single record to the database.
	// The record's ID will be set to the last inserted ID.
	// Returns an error if the operation fails.
	Insert(ctx context.Context, record Record, options ...Options) error

	// InsertMany adds multiple records to the database in a single operation.
	// Returns the number of rows affected and an error if the operation fails.
	// Returns 0, nil if no records are provided.
	InsertMany(ctx context.Context, records []Record, options ...Options) (int64, error)

	// Upsert inserts a record if it doesn't exist, or updates it if it does.
	// Returns true if a new record was inserted, false if an existing record was updated.
	// Returns an error if the operation fails.
	Upsert(ctx context.Context, record Record, options ...Options) (bool, error)

	// GetByID retrieves a single record by its ID.
	// The record parameter should have the ID set, and the method will populate other fields.
	// Returns sql.ErrNoRecordFound if no record exists with the given ID.
	GetByID(ctx context.Context, record Record, options ...Options) error

	// Get retrieves multiple records based on the provided filter.
	// The filter can include conditions, grouping, sorting, limit, and offset.
	// The values slice should contain the parameter values in the order they appear in the filter.
	// The records parameter will be populated with the results.
	Get(ctx context.Context, filter *Filter, values []any, record Records, options ...Options) error

	// UpdateByID updates a record by its ID.
	// The record parameter should have the ID and the fields to update set.
	// Returns true if the record was updated, false if no record exists with the given ID.
	UpdateByID(ctx context.Context, record Record, options ...Options) (bool, error)

	// Update updates records based on the provided condition.
	// The updates parameter specifies which fields to update and their new values.
	// The condition parameter specifies which records to update.
	// The values slice should contain the parameter values in the order they appear in the condition.
	// Returns the number of rows affected and an error if the operation fails.
	Update(ctx context.Context, table *Table, updates *Updates, condition *Condition, values []any, options ...Options) (int64, error)

	// SoftDeleteByID marks a record as deleted by setting its deleted field to true.
	// The table must have a deleted field for this operation to work.
	// Returns true if the record was soft deleted, false if no record exists with the given ID.
	SoftDeleteByID(ctx context.Context, record Record, options ...Options) (bool, error)

	// SoftDelete marks records as deleted based on the provided condition.
	// The table must have a deleted field for this operation to work.
	// The condition parameter specifies which records to soft delete.
	// The values slice should contain the parameter values in the order they appear in the condition.
	// Returns the number of rows affected and an error if the operation fails.
	SoftDelete(ctx context.Context, table *Table, condition *Condition, values []any, options ...Options) (int64, error)

	// DeleteByID permanently removes a record by its ID.
	// Returns true if the record was deleted, false if no record exists with the given ID.
	DeleteByID(ctx context.Context, record Record, options ...Options) (bool, error)

	// Delete permanently removes records based on the provided condition.
	// The condition parameter specifies which records to delete.
	// The values slice should contain the parameter values in the order they appear in the condition.
	// Returns the number of rows affected and an error if the operation fails.
	Delete(ctx context.Context, table *Table, condition *Condition, values []any, options ...Options) (int64, error)

	RunSP(ctx context.Context, spName string, values []any, result SPResult, options ...Options) error
	BeginTransaction(ctx context.Context, options ...Options) (Transaction, error)
}

// Row represents a single row from a database query result.
// It provides a Scan method to extract values from the row.
type Row interface {
	// Scan copies the columns in the current row into the values pointed at by dest.
	// The number of values in dest must be the same as the number of columns in the row.
	Scan(dest ...any) error
}

type Transaction interface {
	// Commit commits the transaction.
	// Returns an error if the commit fails.
	Commit() error
	// Rollback rolls back the transaction.
	// Returns an error if the rollback fails.
	Rollback() error
}

// Rows represents a set of rows from a database query result.
// It extends Row with a Next method to iterate through the rows.
type Rows interface {
	Row
	// Next prepares the next result row for reading with the Scan method.
	// It returns true on success, or false if there is no next result row or an error occurred.
	Next() bool
}

// Record represents a single database record.
// It provides methods for getting/setting the record's ID, table information, and field values.
// Record fields should be exported and have a sql tag for the column name.
type Record interface {
	// ID returns the record's primary key ID.
	ID() int64

	// IdColumn returns the name of the ID column.
	// This is used to exclude the ID column from insert operations.
	IdColumn() string

	// SetID sets the record's primary key ID.
	SetID(id int64)

	// Table returns the table information for this record.
	Table() *Table

	// Columns returns the names of all columns in the record.
	// The ID column should be included in this list.
	Columns() []*Field

	// Values returns the values of all non-ID columns in the record.
	// These values are used for insert and update operations.
	Values() []any

	// Scan populates the record's fields from a database row.
	// The dest parameter should be pointers to the record's fields in the correct order.
	Scan(row Row) error

	// SetDeleted marks the record as deleted (for soft delete operations).
	SetDeleted(deleted bool)
}

type SPResult interface {
	Scan(row Row) error
}

type SPParams struct {
	params map[string]any
}

func NewSPParams() *SPParams {
	return &SPParams{
		params: make(map[string]any),
	}
}

func (p *SPParams) Add(name string, value any) *SPParams {
	p.params[name] = value
	return p
}

func (p *SPParams) Get() map[string]any {
	return p.params
}

// Records represents a collection of database records.
// It provides methods for getting table information and scanning multiple rows.
type Records interface {
	// Table returns the table information for these records.
	Table() *Table

	// Columns returns the names of all columns in the records.
	Columns() []*Field

	// Scan populates the records from a database result set.
	// The rows parameter contains the result rows to scan.
	Scan(rows Rows) error
}

// Table represents a database table with optional joins.
// It's used for building complex queries with multiple table joins.
type Table struct {
	Name  string // The name of the table
	Alias string // Optional alias for the table
	Join  []Join // List of joins with other tables
}

// NewTable creates a new Table instance with the given name.
// The name should be the actual table name in the database.
func NewTable(name string) *Table {
	return &Table{
		Name: name,
		Join: make([]Join, 0),
	}
}

// WithInnerJoin adds an INNER JOIN to the table.
// The on parameter specifies the join condition.
// Returns the table instance for method chaining.
func (t *Table) WithInnerJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
		Type:  InnerJoin,
	})
	return t
}

// WithLeftJoin adds a LEFT JOIN to the table.
// The on parameter specifies the join condition.
// Returns the table instance for method chaining.
func (t *Table) WithLeftJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
		Type:  LeftJoin,
	})
	return t
}

// WithRightJoin adds a RIGHT JOIN to the table.
// The on parameter specifies the join condition.
// Returns the table instance for method chaining.
func (t *Table) WithRightJoin(table *Table, on *Condition) *Table {
	t.Join = append(t.Join, Join{
		Table: table,
		On:    on,
		Type:  RightJoin,
	})
	return t
}

// Order represents the sort order for a field.
type Order int

const (
	Asc  Order = iota // Ascending order
	Desc              // Descending order
)

// OrderBy represents a field and its sort order.
type OrderBy struct {
	Field string // The field name to sort by
	Order Order  // The sort order (Asc or Desc)
}

func NewASCOrder(field string) *OrderBy {
	return &OrderBy{
		Field: field,
		Order: Asc,
	}
}

func NewDESCOrder(field string) *OrderBy {
	return &OrderBy{
		Field: field,
		Order: Desc,
	}
}

// Sort represents a collection of sort criteria.
type Sort struct {
	fields []OrderBy
}

// NewSort creates a new Sort instance.
func NewSort() *Sort {
	return &Sort{
		fields: make([]OrderBy, 0),
	}
}

// Add adds a sort criterion to the sort collection.
// Returns the sort instance for method chaining.
func (o *Sort) Add(field string, order Order) *Sort {
	o.fields = append(o.fields, OrderBy{Field: field, Order: order})
	return o
}

// Fields returns all the sort criteria.
func (o *Sort) Fields() []OrderBy {
	return o.fields
}

// Operator represents the type of comparison or logical operation.
type Operator int

const (
	// Comparison Operators
	EQ  Operator = iota // Equal to (=)
	NEQ                 // Not equal to (<>)
	GT                  // Greater than (>)
	GTE                 // Greater than or equal to (>=)
	LT                  // Less than (<)
	LTE                 // Less than or equal to (<=)
	// Logical Operators
	AND // Both conditions must be true (AND)
	OR  // At least one condition is true (OR)
	NOT // Negates the condition (NOT)
	// Special Operators
	IN         // Value exists in a list (IN)
	NOTIN      // Value does not exist in list (NOT IN)
	LIKE       // Pattern match with wildcards (LIKE)
	NOTLIKE    // Pattern not matching (NOT LIKE)
	ISNULL     // Field is NULL (IS NULL)
	ISNOTNULL  // Field is not NULL (IS NOT NULL)
	EXISTS     // Subquery returns rows (EXISTS)
	NOTEXISTS  // Subquery returns no rows (NOT EXISTS)
	REGEXP     // Matches regular expression (REGEXP)
	BETWEEN    // Value is within range, inclusive (BETWEEN)
	NOTBETWEEN // Value is not within range, inclusive (NOT BETWEEN)
)

// String returns the string representation of the operator.
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

// GroupBy represents grouping criteria for aggregation queries.
type GroupBy struct {
	fields []string
}

// NewGroupBy creates a new GroupBy instance with the specified fields.
func NewGroupBy(fields ...string) *GroupBy {
	return &GroupBy{
		fields: fields,
	}
}

// Fields returns the grouping fields.
func (g *GroupBy) Fields() []string {
	return g.fields
}

// Condition represents a single condition in a WHERE clause.
// It can be a simple comparison or a complex logical operation.
type Condition struct {
	Field      string      // The field name to apply the condition on (e.g., "email", "is_active")
	Value      *Value      // The value to compare against
	Operator   Operator    // The operator to apply (e.g., EQ, NEQ, GT, etc.)
	Conditions []Condition // Nested conditions for AND/OR operations
}

func NewCondition(field string, operator Operator, value *Value) *Condition {
	return &Condition{
		Field:    field,
		Operator: operator,
		Value:    value,
	}
}

func (c *Condition) And(c1 *Condition) *Condition {
	if c.Operator == AND {
		c.Conditions = append(c.Conditions, *c1)
		return c
	}
	return &Condition{
		Operator:   AND,
		Conditions: []Condition{*c, *c1},
	}
}

func (c *Condition) Or(c1 *Condition) *Condition {
	if c.Operator == OR {
		c.Conditions = append(c.Conditions, *c1)
		return c
	}
	return &Condition{
		Operator:   OR,
		Conditions: []Condition{*c, *c1},
	}
}

func NotOf(c *Condition) *Condition {
	return &Condition{
		Operator:   NOT,
		Conditions: []Condition{*c},
	}
}

// Validate checks if the condition is properly configured.
// Returns an error if the condition is invalid.
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

// Filter represents a complete query filter with conditions, grouping, sorting, and pagination.
type Filter struct {
	Condition *Condition // The main condition for the filter
	GroupBy   *GroupBy   // Grouping fields for aggregation
	Sort      *Sort      // Sorting fields for the result set
	Limit     *Value     // Limit the number of records returned
	Offset    *Value     // Offset the number of records returned
}

// UpdateField represents a single field update operation.
type UpdateField struct {
	Field string // The field name to update
	Value *Value // The new value for the field
}

// Updates represents a collection of field updates.
type Updates struct {
	Fields []UpdateField
}

// NewUpdates creates a new Updates instance.
func NewUpdates() *Updates {
	return &Updates{
		Fields: make([]UpdateField, 0),
	}
}

// Add adds a field update to the updates collection.
// Returns the updates instance for method chaining.
func (u *Updates) Add(field string, value *Value) *Updates {
	u.Fields = append(u.Fields, UpdateField{Field: field, Value: value})
	return u
}

type AggregateFunc int

const (
	None AggregateFunc = iota
	Count
	Sum
	Avg
	Min
	Max
)

type Field struct {
	Name     string
	Field    *Field
	Alias    string
	Distinct bool
	Func     AggregateFunc
}

func NewField(name string) *Field {
	return &Field{
		Name: name,
	}
}

func (f *Field) As(alias string) *Field {
	f.Alias = alias
	return f
}

func CountOf(field *Field) *Field {
	return &Field{
		Field: field,
		Func:  Count,
	}
}

func SumOf(field *Field) *Field {
	return &Field{
		Field: field,
		Func:  Sum,
	}
}

func AvgOf(field *Field) *Field {
	return &Field{
		Field: field,
		Func:  Avg,
	}
}

func MinOf(field *Field) *Field {
	return &Field{
		Field: field,
		Func:  Min,
	}
}

func MaxOf(field *Field) *Field {
	return &Field{
		Field: field,
		Func:  Max,
	}
}

func DistinctOf(field *Field) *Field {
	return &Field{
		Field:    field,
		Distinct: true,
	}
}

// JoinType represents the type of join operation.
type JoinType int

const (
	InnerJoin JoinType = iota // INNER JOIN
	LeftJoin                  // LEFT JOIN
	RightJoin                 // RIGHT JOIN
)

// Join represents a table join operation.
type Join struct {
	Type  JoinType   // The type of join (InnerJoin, LeftJoin, RightJoin)
	Table *Table     // The table to join with
	On    *Condition // The join condition
}

// ValueType represents the type of value for validation purposes.
type ValueType int

const (
	Any    ValueType = iota // Any value type
	Column                  // Column reference
)

// Value represents a value in a query condition or update operation.
// It can be a fixed value, a parameterized value, or a column reference.
type Value struct {
	// Type specifies the type of value for validation purposes
	Type ValueType
	// Value is used for fixed values that are not parameterized.
	// These values will be hardcoded in the query.
	// Use this carefully, as it may lead to SQL injection if the value is not sanitized.
	Value any
	// Index is the index of the value in the values slice for parameterized queries.
	// Used when Value is nil and the value comes from a parameter slice.
	Index int
	// Count is used for IN/NOTIN operators to specify the number of values to use from the values slice.
	Count int
}

// WithType sets the value type for validation purposes.
// Returns the value instance for method chaining.
func (v *Value) WithType(t ValueType) *Value {
	v.Type = t
	return v
}

// WithCount sets the count for IN/NOTIN operators.
// Returns the value instance for method chaining.
func (v *Value) WithCount(count int) *Value {
	v.Count = count
	return v
}

// IsColumn returns true if the value is a column reference.
func (v *Value) IsColumn() bool {
	return v.Type == Column
}

// IsStringValue returns true if the value is a string type.
func (v *Value) IsStringValue() bool {
	if v.Value == nil {
		return false
	}
	return reflect.TypeOf(v.Value).Kind() == reflect.String
}

// IsValue returns true if the value has a fixed value set.
func (v *Value) IsValue() bool {
	return v.Value != nil
}

// NewIndexedValue creates a new Value with the specified index.
// This is used for parameterized queries where the value is not fixed and is passed as a parameter.
// For IN/NOTIN operators, use WithCount to specify the number of values to use from the values slice.
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

// NewColumnValue creates a new Value that references a column.
// This is used for conditions that compare one column to another.
func NewColumnValue(column string) *Value {
	return &Value{
		Type:  Column,
		Value: column,
	}
}

// GetValues extracts values from a slice based on the provided indexes.
// It handles both scalar values and array values (for IN/NOTIN operators).
// Assumes validation has already been applied to ensure valuesPassed has enough values.
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

// Options provides additional configuration for database operations.
// It can be used to specify database selection, query preparation, and timeouts.
type Options struct {
	// UsePrimaryDB specifies whether to use the primary database.
	// No need to set this for write operations as they default to the primary database.
	UsePrimaryDB bool
	// PreparedName specifies a unique name for prepared statement caching.
	// If set, the query will be prepared and cached for reuse.
	PreparedName string
	// Timeout specifies the query execution timeout in milliseconds.
	Timeout int64
	// Transaction specifies whether to run the operation within a transaction.
	Transaction Transaction
}

// GetOptions returns the first option from the options slice if available,
// otherwise returns an empty Options struct.
// This is used internally by the library.
func GetOptions(options ...Options) Options {
	if len(options) > 0 {
		return options[0]
	}
	return Options{}
}
