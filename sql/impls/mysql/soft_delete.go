package mysql

import (
	"context"

	"github.com/gofreego/database/sql"
)

// SoftDelete implements sql.Database.
func (c *MysqlDatabase) SoftDelete(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	panic("unimplemented")
}
