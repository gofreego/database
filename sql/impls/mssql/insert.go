package mssql

import (
	"context"
	driver "database/sql"
	"fmt"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

func (c *MssqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	if record == nil {
		return sql.NewInvalidQueryError("record is nil")
	}

	opt := sql.GetOptions(options...)
	var err error
	var query string
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
			_, err = txn.ExecContext(ctx, stmt.GetQuery(), record.Values()...)
		} else {
			_, err = stmt.GetStatement().ExecContext(ctx, record.Values()...)
		}

	} else {
		query, _, err = c.parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		// if transaction is provided, use it to execute the query
		if opt.Transaction != nil {
			var txn *driver.Tx
			txn, err = internal.GetTransaction(opt.Transaction)
			if err != nil {
				return err
			}
			_, err = txn.ExecContext(ctx, query, record.Values()...)
		} else {
			_, err = c.db.ExecContext(ctx, query, record.Values()...)
		}
	}
	if err != nil {
		return internal.HandleError(err)
	}
	// For MSSQL, we need to use OUTPUT clause to get the inserted ID
	// Execute a separate query to get the last inserted ID using OUTPUT
	idColumn := record.IdColumn()
	tableName := record.Table().Name
	outputQuery := fmt.Sprintf("SELECT TOP 1 %s FROM %s ORDER BY %s DESC", idColumn, tableName, idColumn)

	var id int64
	if opt.Transaction != nil {
		var txn *driver.Tx
		txn, err = internal.GetTransaction(opt.Transaction)
		if err != nil {
			return err
		}
		err = txn.QueryRowContext(ctx, outputQuery).Scan(&id)
	} else {
		err = c.db.QueryRowContext(ctx, outputQuery).Scan(&id)
	}
	if err != nil {
		return internal.HandleError(err)
	}
	record.SetID(id)
	return nil
}
