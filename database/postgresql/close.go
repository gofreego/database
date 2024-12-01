package postgresql

import "context"

func (d *Database) Close(ctx context.Context) error {
	return d.conn.Close()
}
