package common

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *Executor) Get(ctx context.Context, filter *sql.Filter, values []any, records sql.Records, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var filterIndexes []int
	var rows sql.Rows
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				var query string
				query, filterIndexes, err = c.parser.ParseGetByFilterQuery(filter, records)
				if err != nil {
					return internal.HandleError(err)
				}
				logger.Debug(ctx, "GetByFilter query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithValueIndexes(filterIndexes).WithQuery(query)
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
			rows, err = txn.QueryContext(ctx, stmt.GetQuery(), sql.GetValues(stmt.GetValueIndexes(), values)...)
		} else {
			rows, err = stmt.GetStatement().QueryContext(ctx, sql.GetValues(stmt.GetValueIndexes(), values)...)
		}
	} else {
		// if prepared statement is not provided, parse the query and execute it
		var query string
		query, filterIndexes, err = c.parser.ParseGetByFilterQuery(filter, records)
		if err != nil {
			return internal.HandleError(err)
		}
		logger.Debug(ctx, "GetByFilter query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return err
			}
			rows, err = txn.QueryContext(ctx, query, sql.GetValues(filterIndexes, values)...)
		} else {
			rows, err = c.db.QueryContext(ctx, query, sql.GetValues(filterIndexes, values)...)
		}
	}
	if err != nil {
		return internal.HandleError(err)
	}
	return internal.HandleError(records.Scan(rows))
}

func (c *Executor) GetByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var row *driver.Row
	var err error
	var query string
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, err = c.parser.ParseGetByIDQuery(record)
				if err != nil {
					return internal.HandleError(err)
				}
				logger.Debug(ctx, "GetByID query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithQuery(query)
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
			row = txn.QueryRowContext(ctx, stmt.GetQuery(), record.ID())
		} else {
			row = stmt.GetStatement().QueryRowContext(ctx, record.ID())
		}
	} else {
		query, err = c.parser.ParseGetByIDQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		logger.Debug(ctx, "GetByID query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return err
			}
			row = txn.QueryRowContext(ctx, query, record.ID())
		} else {
			row = c.db.QueryRowContext(ctx, query, record.ID())
		}
	}
	if row.Err() != nil {
		return internal.HandleError(row.Err())
	}
	return internal.HandleError(record.Scan(row))
}
