package common

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *Executor) RunSP(ctx context.Context, spName string, values []any, result sql.SPResult, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var row *driver.Row
	var query string
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, err = c.parser.ParseSPQuery(spName, values)
				if err != nil {
					return internal.HandleError(err)
				}
				logger.Debug(ctx, "GetByFilter query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps)
				c.preparedStatements.Add(opt.PreparedName, stmt)
			}
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return err
			}
			row = txn.QueryRowContext(ctx, stmt.GetQuery(), values...)
		} else {
			row = stmt.GetStatement().QueryRowContext(ctx, values...)
		}
	} else {
		query, err = c.parser.ParseSPQuery(spName, values)
		if err != nil {
			return internal.HandleError(err)
		}
		logger.Debug(ctx, "SP query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return err
			}
			row = txn.QueryRowContext(ctx, query, values...)
		} else {
			row = c.db.QueryRowContext(ctx, query, values...)
		}
	}
	if row.Err() != nil {
		return internal.HandleError(row.Err())
	}
	return internal.HandleError(result.Scan(row))
}
