package sql

type UnimplementedRecord struct {
}

func (u *UnimplementedRecord) ID() int64 {
	panic("ID method is not implemented")
}

func (u *UnimplementedRecord) IdColumn() string {
	panic("IdColumn method is not implemented")
}

func (u *UnimplementedRecord) SetID(id int64) {
	panic("SetID method is not implemented")
}

func (u *UnimplementedRecord) Table() *Table {
	panic("Table method is not implemented")
}

func (u *UnimplementedRecord) Columns() []string {
	panic("Columns method is not implemented")
}

func (u *UnimplementedRecord) Values() []any {
	panic("Values method is not implemented")
}

func (u *UnimplementedRecord) Scan(row Row) error {
	panic("Scan method is not implemented")
}

func (u *UnimplementedRecord) SetDeleted(deleted bool) {
	panic("SetDeleted method is not implemented")
}
