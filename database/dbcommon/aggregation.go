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
}
