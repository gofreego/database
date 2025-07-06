package mysql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
)

/*
Upsert inserts a record into the database if it doesn't exist, otherwise it updates the record.
Returns the number of rows affected and an error if any.
Returns 0, nil if no records are provided.
Returns 0, sql.ErrNoRecordInserted if no records are inserted.
*/

func (c *MysqlDatabase) Upsert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *db.Stmt
		var ok bool
		var query string
		var values []any
		if stmt, ok = c.preparedStatements[opt.PreparedName]; !ok {
			query, values, err = parser.ParseUpsertQuery(record)
			if err != nil {
				return handleError(err)
			}
			stmt, err = c.db.PrepareContext(ctx, query)
			if err != nil {
				return handleError(err)
			}
			c.preparedStatements[opt.PreparedName] = stmt
		}
		res, err = stmt.ExecContext(ctx, values...)
		if err != nil {
			return handleError(err)
		}
	} else {
		query, values, err := parser.ParseUpsertQuery(record)
		if err != nil {
			return handleError(err)
		}
		res, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return handleError(err)
		}
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return handleError(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRecordInserted
	}
	id, err := res.LastInsertId()
	if err != nil {
		return handleError(err)
	}
	record.SetID(id)
	return nil
}
