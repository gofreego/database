package dbcommon

type Order int

const (
	Asc Order = iota
	Desc
)

type Sort struct {
	Column string
	Order  Order
}
