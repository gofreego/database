package common

import (
	"context"
	db "database/sql"

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
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
		var values []any
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
		res, err = stmt.GetStatement().ExecContext(ctx, values...)
		if err != nil {
			return false, internal.HandleError(err)
		}
	} else {
		query, values, err := c.parser.ParseUpsertQuery(record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		res, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return false, internal.HandleError(err)
		}
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
