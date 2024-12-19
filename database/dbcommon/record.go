package dbcommon

type Record interface {
	TableName() string
	InsertColumnsValues() ([]string, []interface{})
	UpdateColumnsValues() ([]string, []interface{})
	SelectColumns() []string
	ID() any
	SetID(id any)
	ScanRow(row Row) error
}

type Records interface {
	TableName() string
	SelectColumns() []string
	ScanRows(row Rows) error
}
