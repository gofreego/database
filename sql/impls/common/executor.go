package common

import (
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
}

type Executor struct {
	db                 *driver.DB
	parser             Parser
	preparedStatements internal.PreparedStatements
}

func NewExecutor(conn *driver.DB, parser Parser) *Executor {
	return &Executor{
		db:                 conn,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}
}
