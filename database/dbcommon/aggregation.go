package dbcommon

type Function int

const (
	NoFunction Function = iota
	Sum
	Avg
	Count
	Max
	Min
	Distinct
)

type Column struct {
	Column   string
	Alias    string
	Function Function
}

type AggregationRecords interface {
	AggregationColumns() []*Column
	Table() *Table
	ScanRows(row Rows) error
}

type Aggregator interface {
	Filter
	GroupBy() []string
}

func SumOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Sum,
	}
}

func AvgOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Avg,
	}
}

func CountOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Count,
	}
}

func MaxOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Max,
	}
}

func MinOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Min,
	}
}

func DistinctOf(column string) *Column {
	return &Column{
		Column:   column,
		Function: Distinct,
	}
}
