package mssql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
)

// Upsert inserts a record into the database if it doesn't exist, otherwise it updates the record.
// Returns true if a row was inserted or updated, false otherwise.
func (c *MssqlDatabase) Upsert(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
		var values []any
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, values, err = parser.ParseUpsertQuery(record)
			if err != nil {
				return false, internal.HandleError(err)
			}
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return false, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		res, err = stmt.GetStatement().ExecContext(ctx, values...)
		if err != nil {
			return false, internal.HandleError(err)
		}
	} else {
		query, values, err := parser.ParseUpsertQuery(record)
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
	if err == nil {
		record.SetID(id)
	}
	return true, nil
}
