package common

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

// DeleteByID implements sql.Database.
func (c *Executor) DeleteByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	// if prepared name is not empty, use prepared statement
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, err = c.parser.ParseDeleteByIDQuery(record)
				if err != nil {
					return false, internal.HandleError(err)
				}
				logger.Debug(ctx, "DeleteByID query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return false, internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithQuery(query)
				c.preparedStatements.Add(opt.PreparedName, stmt)
			}
		}
		// execute the prepared statement
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			result, err = txn.ExecContext(ctx, stmt.GetQuery(), record.ID())
		} else {
			result, err = stmt.GetStatement().ExecContext(ctx, record.ID())
		}
	} else {
		// if prepared name is empty, parse the query and execute the query
		query, err = c.parser.ParseDeleteByIDQuery(record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		logger.Debug(ctx, "DeleteByID query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			result, err = txn.ExecContext(ctx, query, record.ID())
		} else {
			result, err = c.db.ExecContext(ctx, query, record.ID())
		}
	}
	// if there is an error, return false and the error
	if err != nil {
		return false, internal.HandleError(err)
	}
	// get the number of rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, internal.HandleError(err)
	}
	// if the number of rows affected is greater than 0, return true, otherwise return false
	return rowsAffected > 0, nil
}

// Delete implements sql.Database.
func (c *Executor) Delete(ctx context.Context, table *sql.Table, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, valueIndexes, err = c.parser.ParseDeleteQuery(table, condition)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				logger.Debug(ctx, "Delete query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes).WithQuery(query)
				c.preparedStatements.Add(opt.PreparedName, stmt)
			}
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return 0, err
			}
			result, err = txn.ExecContext(ctx, stmt.GetQuery(), sql.GetValues(valueIndexes, values)...)
		} else {
			result, err = stmt.GetStatement().ExecContext(ctx, sql.GetValues(valueIndexes, values)...)
		}
	} else {
		query, valueIndexes, err = c.parser.ParseDeleteQuery(table, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Delete query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return 0, err
			}
			result, err = txn.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
		} else {
			result, err = c.db.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
		}
	}
	if err != nil {
		return 0, internal.HandleError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, internal.HandleError(err)
	}
	return rowsAffected, nil
}

func (c *Executor) SoftDeleteByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, err = c.parser.ParseSoftDeleteByIDQuery(record.Table(), record)
				if err != nil {
					return false, internal.HandleError(err)
				}
				logger.Debug(ctx, "Soft delete by id query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return false, internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes).WithQuery(query)
				c.preparedStatements.Add(opt.PreparedName, stmt)
			}
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			result, err = txn.ExecContext(ctx, stmt.GetQuery(), record.ID())
		} else {
			result, err = stmt.GetStatement().ExecContext(ctx, record.ID())
		}
	} else {
		query, err = c.parser.ParseSoftDeleteByIDQuery(record.Table(), record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		logger.Debug(ctx, "Soft delete by id query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			result, err = txn.ExecContext(ctx, query, record.ID())
		} else {
			result, err = c.db.ExecContext(ctx, query, record.ID())
		}
	}
	if err != nil {
		return false, internal.HandleError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, internal.HandleError(err)
	}
	return rowsAffected > 0, nil
}

func (c *Executor) SoftDelete(ctx context.Context, table *sql.Table, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, valueIndexes, err = c.parser.ParseSoftDeleteQuery(table, condition)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				logger.Debug(ctx, "Soft delete query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes).WithQuery(query)
				c.preparedStatements.Add(opt.PreparedName, stmt)
			}
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return 0, err
			}
			result, err = txn.ExecContext(ctx, stmt.GetQuery(), sql.GetValues(valueIndexes, values)...)
		} else {
			result, err = stmt.GetStatement().ExecContext(ctx, sql.GetValues(valueIndexes, values)...)
		}
	} else {
		query, valueIndexes, err = c.parser.ParseSoftDeleteQuery(table, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Soft delete query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return 0, err
			}
			result, err = txn.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
		} else {
			result, err = c.db.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
		}
	}
	if err != nil {
		return 0, internal.HandleError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, internal.HandleError(err)
	}
	return rowsAffected, nil
}
