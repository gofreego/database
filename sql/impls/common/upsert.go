package common

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

/*
Upsert inserts a record into the database if it doesn't exist, otherwise it updates the record.
Returns the number of rows affected and an error if any.
Returns 0, nil if no records are provided.
Returns 0, sql.ErrNoRecordInserted if no records are inserted.
*/

func (c *Executor) Upsert(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var res driver.Result
	var query string
	var values []any
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, values, err = c.parser.ParseUpsertQuery(record)
				if err != nil {
					return false, internal.HandleError(err)
				}
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return false, internal.HandleError(err)
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
		query, values, err = c.parser.ParseUpsertQuery(record)
		if err != nil {
			return false, internal.HandleError(err)
		}
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
	// handle error
	if err != nil {
		return false, internal.HandleError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, internal.HandleError(err)
	}
	if rowsAffected == 0 {
		return false, sql.ErrNoRecordInserted
	}
	id, err := res.LastInsertId()
	if err != nil {
		return false, internal.HandleError(err)
	}
	record.SetID(id)
	return true, nil
}
