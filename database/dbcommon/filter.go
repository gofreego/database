package dbcommon

type Filter interface {
	Condition() *Condition
	Sorts() []Sort
	GroupBy() []string
	Limit() int64
	Offset() int64
}
