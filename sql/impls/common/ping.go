package common

import (
	"context"
)

func (c *Executor) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}
