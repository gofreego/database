package postgresql

import (
	"context"
	"database/sql"
)

type Database struct {
	conn               *sql.DB
	preparedStatements map[string]*sql.Stmt
}

func NewDatabase(ctx context.Context, conf *Config) (*Database, error) {
	conn, err := newConnection(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &Database{
		conn: conn,
	}, nil
}
