package postgresql

import "context"

func (d *Database) Ping(ctx context.Context) error {
	return d.conn.PingContext(ctx)
}
