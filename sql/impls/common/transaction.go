package common

import (
	"context"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

func (c *Executor) BeginTransaction(ctx context.Context, options ...sql.Options) (sql.Transaction, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, internal.HandleError(err)
	}
	return tx, nil
}
