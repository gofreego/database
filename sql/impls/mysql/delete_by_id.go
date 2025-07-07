package mysql

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

// DeleteByID implements sql.Database.
func (c *MysqlDatabase) DeleteByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	// if prepared name is not empty, use prepared statement
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		// if prepared statement is not found, parse the query and create a new prepared statement
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, err = parser.ParseDeleteByIDQuery(record)
			if err != nil {
				return false, internal.HandleError(err)
			}
			logger.Debug(ctx, "DeleteByID query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return false, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		// execute the prepared statement
		result, err = stmt.GetStatement().ExecContext(ctx, record.ID())
	} else {
		// if prepared name is empty, parse the query and execute the query
		query, err = parser.ParseDeleteByIDQuery(record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		logger.Debug(ctx, "DeleteByID query: %s", query)
		result, err = c.db.ExecContext(ctx, query, record.ID())
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
