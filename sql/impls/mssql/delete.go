package mssql

import (
	"context"
	driver "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *MssqlDatabase) DeleteByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
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
		result, err = stmt.GetStatement().ExecContext(ctx, record.ID())
	} else {
		query, err = parser.ParseDeleteByIDQuery(record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		logger.Debug(ctx, "DeleteByID query: %s", query)
		result, err = c.db.ExecContext(ctx, query, record.ID())
	}
	if err != nil {
		return false, internal.HandleError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, internal.HandleError(err)
	}
	return rowsAffected > 0, nil
}

func (c *MssqlDatabase) Delete(ctx context.Context, table *sql.Table, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, valueIndexes, err = parser.ParseDeleteQuery(table, condition)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			logger.Debug(ctx, "Delete query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		result, err = stmt.GetStatement().ExecContext(ctx, sql.GetValues(valueIndexes, values)...)
	} else {
		query, valueIndexes, err = parser.ParseDeleteQuery(table, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Delete query: %s", query)
		result, err = c.db.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
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

func (c *MssqlDatabase) SoftDeleteByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, err = parser.ParseSoftDeleteByIDQuery(record.Table(), record)
			if err != nil {
				return false, internal.HandleError(err)
			}
			logger.Debug(ctx, "Soft delete by id query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return false, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		result, err = stmt.GetStatement().ExecContext(ctx, record.ID())
	} else {
		query, err = parser.ParseSoftDeleteByIDQuery(record.Table(), record)
		if err != nil {
			return false, internal.HandleError(err)
		}
		logger.Debug(ctx, "Soft delete by id query: %s", query)
		result, err = c.db.ExecContext(ctx, query, record.ID())
	}
	if err != nil {
		return false, internal.HandleError(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, internal.HandleError(err)
	}
	return rowsAffected > 0, nil
}

func (c *MssqlDatabase) SoftDelete(ctx context.Context, table *sql.Table, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	opt := sql.GetOptions(options...)
	var err error
	var result driver.Result
	var query string
	var valueIndexes []int
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, valueIndexes, err = parser.ParseSoftDeleteQuery(table, condition)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			logger.Debug(ctx, "Soft delete query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		result, err = stmt.GetStatement().ExecContext(ctx, sql.GetValues(valueIndexes, values)...)
	} else {
		query, valueIndexes, err = parser.ParseSoftDeleteQuery(table, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Soft delete query: %s", query)
		result, err = c.db.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
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
