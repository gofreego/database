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

type AggregationColumn struct {
	Column   string
	Alias    string
	Function Function
}

type AggregationRecords interface {
	AggregationColumns() []*AggregationColumn
	TableName() string
	ScanRows(row Rows) error
}

type Aggregator interface {
	Filter
	GroupBy() []string
}

func SumOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Sum,
	}
}

func AvgOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Avg,
	}
}

func CountOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Count,
	}
}

func MaxOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Max,
	}
}

func MinOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Min,
	}
}

func DistinctOf(column string) *AggregationColumn {
	return &AggregationColumn{
		Column:   column,
		Function: Distinct,
	}
}
