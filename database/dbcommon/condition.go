package dbcommon

type Operator int

const (
	EQ Operator = iota
	NEQ
	GT
	GTE
	LT
	LTE
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

func Equal(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: EQ, Value: value}
}

func NotEqual(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: NEQ, Value: value}
}

func GreaterThan(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: GT, Value: value}
}

func GreaterThanOrEqual(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: GTE, Value: value}
}

func LessThan(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: LT, Value: value}
}

func LessThanOrEqual(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: LTE, Value: value}
}

func LikeCondition(column string, value interface{}) *Condition {
	return &Condition{Column: column, Operator: Like, Value: value}
}

func InCondition(column string, values ...interface{}) *Condition {
	return &Condition{Column: column, Operator: In, Values: values}
}

func NotInCondition(column string, values ...interface{}) *Condition {
	return &Condition{Column: column, Operator: NotIn, Values: values}
}

func OrCondition(conditions ...*Condition) *Condition {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return &Condition{Operator: OR, Conditions: conditions}
}

func AndCondition(conditions ...*Condition) *Condition {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return &Condition{Operator: AND, Conditions: conditions}
}
