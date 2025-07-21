package common

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

type Parser interface {
	ParseDeleteByIDQuery(record sql.Record) (string, error)
	ParseDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error)
	ParseSoftDeleteByIDQuery(table *sql.Table, record sql.Record) (string, error)
	ParseSoftDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error)
	ParseGetByIDQuery(record sql.Record) (string, error)
	ParseGetByFilterQuery(filter *sql.Filter, records sql.Records) (string, []int, error)
	ParseInsertQuery(record ...sql.Record) (string, []any, error)
	ParseUpdateByIDQuery(record sql.Record) (string, error)
	ParseUpdateQuery(table *sql.Table, updates *sql.Updates, condition *sql.Condition) (string, []int, error)
	ParseUpsertQuery(record sql.Record) (string, []any, error)
	ParseSPQuery(spName string, values []any) (string, error)
}

type DB interface {
	Close() error
	PingContext(ctx context.Context) error
	PrepareContext(ctx context.Context, query string) (*driver.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (driver.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*driver.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *driver.Row
	BeginTx(ctx context.Context, opts *driver.TxOptions) (*driver.Tx, error)
}

type Executor struct {
	db                 DB
	parser             Parser
	preparedStatements internal.PreparedStatements
}

func NewExecutor(conn DB, parser Parser) *Executor {
	return &Executor{
		db:                 conn,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}
}
