package dbcommon

type Row interface {
	Scan(dest ...interface{}) error
}
