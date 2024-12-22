package dbcommon

type Record interface {
	Table() *Table
	InsertColumnsValues() ([]string, []interface{})
	UpdateColumnsValues() ([]string, []interface{})
	SelectColumns() []string
	ID() any
	SetID(id any)
	ScanRow(row Row) error
}

type Records interface {
	Table() *Table
	SelectColumns() []string
	ScanRows(row Rows) error
}
