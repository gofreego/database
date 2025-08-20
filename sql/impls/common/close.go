package common

import (
	"context"

	"github.com/gofreego/database/sql/internal"
)

func (c *Executor) Close(ctx context.Context) error {
	c.preparedStatements.Close()
	return internal.HandleError(c.db.Close())
}
