package dbcommon

type Filter interface {
	Condition() *Condition
	Sorts() []Sort
	Limit() int64
	Offset() int64
}
