package common

import (
	"context"

	"github.com/gofreego/database/sql/internal"
)

func (c *Executor) Ping(ctx context.Context) error {
	return internal.HandleError(c.db.PingContext(ctx))
}
