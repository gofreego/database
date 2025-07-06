package mysql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/internal"
)

/*
Insert inserts a record into the database.
Returns an error if any.
It will set the ID of the record to the last inserted ID.
*/
func (c *MysqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, _, err = parser.ParseInsertQuery(record)
			if err != nil {
				return internal.HandleError(err)
			}
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements[opt.PreparedName] = stmt
		}

		res, err = stmt.GetStatement().ExecContext(ctx, record.Values()...)
		if err != nil {
			return internal.HandleError(err)
		}
	} else {
		query, values, err := parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		res, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return internal.HandleError(err)
		}
	}
	id, err := res.LastInsertId()
	if err != nil {
		return internal.HandleError(err)
	}
	record.SetID(id)
	return nil
}
