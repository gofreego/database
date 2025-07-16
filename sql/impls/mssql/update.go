package mssql

import (
	"context"
	driver "database/sql"
	"fmt"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

// UpdateByID updates a record in the database by its ID.
// It updates all fields except the id column, using the id column in the WHERE clause.
// Returns true if a row was updated, false otherwise.
func (c *MssqlDatabase) UpdateByID(ctx context.Context, record sql.Record, options ...sql.Options) (bool, error) {
	if record == nil {
		return false, sql.NewInvalidQueryError("update by id:: record cannot be nil")
	}

	opt := sql.GetOptions(options...)
	values := record.Values()
	values = append(values, record.ID())

	var res driver.Result
	var err error
	var query string
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool

		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, err = parser.ParseUpdateByIDQuery(record)
			if err != nil {
				return false, err
			}
			logger.Debug(ctx, "UpdateByID :: prepareName: %s ,query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return false, fmt.Errorf("UpdateByID prepare failed: %w", err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		res, err = stmt.GetStatement().ExecContext(ctx, values...)
		if err != nil {
			return false, fmt.Errorf("UpdateByID exec prepared failed: %w", err)
		}
	} else {
		query, err = parser.ParseUpdateByIDQuery(record)
		if err != nil {
			return false, err
		}
		logger.Debug(ctx, "UpdateByID query:: %s", query)
		res, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return false, fmt.Errorf("UpdateByID failed: %w", err)
		}
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("UpdateByID RowsAffected failed: %w", err)
	}
	return rowsAffected > 0, nil
}

func (c *MssqlDatabase) Update(ctx context.Context, table *sql.Table, updates *sql.Updates, condition *sql.Condition, values []any, options ...sql.Options) (int64, error) {
	if table == nil {
		return 0, sql.NewInvalidQueryError("update query:: table cannot be nil")
	}
	if updates == nil {
		return 0, sql.NewInvalidQueryError("update query:: updates cannot be nil")
	}

	opt := sql.GetOptions(options...)
	var err error
	var res driver.Result
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, valueIndexes, err := parser.ParseUpdateQuery(table, updates, condition)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			logger.Debug(ctx, "Update query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(valueIndexes)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		res, err = stmt.GetStatement().ExecContext(ctx, sql.GetValues(stmt.GetValueIndexes(), values)...)
		if err != nil {
			return 0, internal.HandleError(err)
		}
	} else {
		query, valueIndexes, err := parser.ParseUpdateQuery(table, updates, condition)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		logger.Debug(ctx, "Update query: %s", query)
		res, err = c.db.ExecContext(ctx, query, sql.GetValues(valueIndexes, values)...)
		if err != nil {
			return 0, internal.HandleError(err)
		}
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, internal.HandleError(err)
	}
	return rowsAffected, nil
}
