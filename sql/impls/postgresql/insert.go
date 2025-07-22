package postgresql

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

func (c *PostgresqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var query string
	var row *driver.Row
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		{
			if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
				query, _, err = c.parser.ParseInsertQuery(record)
				if err != nil {
					return internal.HandleError(err)
				}
				// Add RETURNING clause for PostgreSQL
				query += " RETURNING id"
				ps, err := c.db.PrepareContext(ctx, query)
				if err != nil {
					return internal.HandleError(err)
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
				return err
			}
			row = txn.QueryRowContext(ctx, stmt.GetQuery(), record.Values()...)
		} else {
			row = stmt.GetStatement().QueryRowContext(ctx, record.Values()...)
		}

	} else {
		query, values, err := c.parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		// Add RETURNING clause for PostgreSQL
		query += " RETURNING id"
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
	if err = row.Err(); err != nil {
		return internal.HandleError(err)
	}
	var id int64
	if err = row.Scan(&id); err != nil {
		return internal.HandleError(err)
	}
	record.SetID(id)
	return nil
}
