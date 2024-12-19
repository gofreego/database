package dbcommon

type Row interface {
	Scan(dest ...interface{}) error
}

type Rows interface {
	Next() bool
	Row
}
