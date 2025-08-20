package internal

import (
	"context"
	"errors"

	driver "database/sql"
)

type DB interface {
	Close() error
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*driver.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (driver.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*driver.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *driver.Row
	BeginTx(ctx context.Context, opts *driver.TxOptions) (*driver.Tx, error)
}

func GetTransaction(tx any) (*driver.Tx, error) {
	if tx == nil {
		return nil, errors.New("transaction is nil")
	}
	txx, ok := tx.(*driver.Tx)
	if !ok {
		return nil, errors.New("invalid transaction object")
	}
	return txx, nil
}
