package mysql

import (
	"context"

	"github.com/gofreego/database/sql"
)

// Delete implements sql.Database.
func (c *MysqlDatabase) Delete(ctx context.Context, table sql.Table, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	panic("unimplemented")
}
