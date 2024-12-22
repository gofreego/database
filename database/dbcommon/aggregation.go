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
	AggregationColumns() []AggregationColumn
	TableName() string
	ScanRows(row Rows) error
}

type Aggregator interface {
	Filter
	GroupBy() []string
}

func SumOf(column string) *AggregationColumn{
	return &AggregationColumn{
		Column: column
	}
}