package common

import (
	"context"
	driver "database/sql"
	"fmt"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

// UpdateByID updates a record in the database by its ID.
// It updates all fields except the id column, using the id column in the WHERE clause.
// Returns true if a row was updated, false otherwise.
func (c *Executor) UpdateByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	if record == nil {
		return false, sql.NewInvalidQueryError("update by id:: record cannot be nil")
	}

	opt := sql.GetOptions(options...)
	values := record.Values()
	values = append(values, record.ID())

	var res driver.Result
	var err error
	var query string
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, err = c.parser.ParseUpdateByIDQuery(record)
				if err != nil {
					return false, err
				}
				logger.Debug(ctx, "UpdateByID :: prepareName: %s ,query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return false, fmt.Errorf("UpdateByID prepare failed: %w", err)
				}
				stmt = internal.NewPreparedStatement(ps)
				c.preparedStatements[opt.PreparedName] = stmt
			}
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			res, err = txn.ExecContext(ctx, stmt.GetQuery(), values...)
		} else {
			res, err = stmt.GetStatement().ExecContext(ctx, values...)
		}

	} else {
		query, err = c.parser.ParseUpdateByIDQuery(record)
		if err != nil {
			return false, err
		}
		logger.Debug(ctx, "UpdateByID query: %s", query)
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return false, err
			}
			res, err = txn.ExecContext(ctx, query, values...)
		} else {
			res, err = c.db.ExecContext(ctx, query, values...)
		}
	}
	if err != nil {
		return false, internal.HandleError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("UpdateByID RowsAffected failed: %w", err)
	}
	return rowsAffected > 0, nil
}

// Update implements sql.Database.
func (c *Executor) Update(ctx context.Context, table *sql.Table, updates *sql.Updates, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
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
				query, valueIndexes, err = c.parser.ParseUpdateQuery(table, updates, condition)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				logger.Debug(ctx, "Update query: %s", query)
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return 0, internal.HandleError(err)
				}
				stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes)
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
		query, valueIndexes, err = c.parser.ParseUpdateQuery(table, updates, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Update query: %s", query)
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
