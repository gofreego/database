package dbcommon

type Operator int

const (
	Equal Operator = iota
	NotEqual
	GreaterThan
	GreaterThanOrEqual
	LessThan
	LessThanOrEqual
	Like
	In
	NotIn
	OR
	AND
)

type Condition struct {
	Column     string
	Operator   Operator
	Value      interface{}
	Values     []interface{}
	Conditions []*Condition
}
