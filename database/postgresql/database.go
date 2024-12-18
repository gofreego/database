package postgresql

import (
	"context"
	"database/sql"
	"openauth/database/database/dberrors"
)

type Database struct {
	conn               *sql.DB
	preparedStatements map[string]*sql.Stmt
}

func NewDatabase(ctx context.Context, conf *Config) (*Database, error) {
	if conf == nil {
		return nil, dberrors.NewError(dberrors.ErrInvalidConfig, "No config provided for Database.PostgreSQL", nil)
	}
	conn, err := newConnection(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &Database{
		conn: conn,
	}, nil
}