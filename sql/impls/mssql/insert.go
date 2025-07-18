package mssql

import (
	"context"
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
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
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

		_, err = stmt.GetStatement().ExecContext(ctx, record.Values()...)
		if err != nil {
			return internal.HandleError(err)
		}
	} else {
		query, values, err := c.parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		_, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return internal.HandleError(err)
		}
	}

	// For MSSQL, we need to use OUTPUT clause to get the inserted ID
	// Execute a separate query to get the last inserted ID using OUTPUT
	idColumn := record.IdColumn()
	tableName := record.Table().Name
	outputQuery := fmt.Sprintf("SELECT TOP 1 %s FROM %s ORDER BY %s DESC", idColumn, tableName, idColumn)

	var id int64
	err = c.db.QueryRowContext(ctx, outputQuery).Scan(&id)
	if err != nil {
		return internal.HandleError(err)
	}
	record.SetID(id)
	return nil
}
