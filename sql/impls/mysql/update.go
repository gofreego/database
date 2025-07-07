package mysql

import (
	"context"

	"github.com/gofreego/database/sql"
)

// Update implements sql.Database.
func (c *MysqlDatabase) Update(ctx context.Context, table sql.Table, updates *sql.Updates, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	panic("unimplemented")
}
