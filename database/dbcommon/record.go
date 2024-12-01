package dbcommon

import "database/sql"

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
	ScanRows(row *sql.Rows) error
}
