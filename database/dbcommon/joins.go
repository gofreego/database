package dbcommon

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

type Join struct {
	JoinType JoinType
	Table    *Table
	On       *Condition
}

func InnerJoinWith(table *Table, on *Condition) *Join {
	return &Join{JoinType: InnerJoin, Table: table, On: on}
}

func LeftJoinWith(table *Table, on *Condition) *Join {
	return &Join{JoinType: LeftJoin, Table: table, On: on}
}

func RightJoinWith(table *Table, on *Condition) *Join {
	return &Join{JoinType: RightJoin, Table: table, On: on}
}

func FullJoinWith(table *Table, on *Condition) *Join {
	return &Join{JoinType: FullJoin, Table: table, On: on}
}

type Table struct {
	Name  string
	Alias string
	Joins []*Join
}

func NewTable(name string) *Table {
	return &Table{Name: name}
}

func (t *Table) WithAlias(alias string) *Table {
	t.Alias = alias
	return t
}

func (t *Table) WithJoins(joins ...*Join) *Table {
	t.Joins = joins
	return t
}
