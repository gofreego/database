package mysql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/goutils/logger"
)

func (c *MysqlDatabase) GetByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	if opt.PreparedName != "" {
		var stmt *db.Stmt
		var ok bool

		if stmt, ok = c.preparedStatements[opt.PreparedName]; !ok {
			query, err := parser.ParseGetByIDQuery(record)
			if err != nil {
				return handleError(err)
			}
			logger.Debug(ctx, "GetByID query: %s", query)
			stmt, err = c.db.PrepareContext(ctx, query)
			if err != nil {
				return handleError(err)
			}
			c.preparedStatements[opt.PreparedName] = stmt
		}

		row := stmt.QueryRowContext(ctx, record.ID())
		if err = row.Err(); err != nil {
			return handleError(err)
		}
		return handleError(record.Scan(row))
	}
	query, err := parser.ParseGetByIDQuery(record)
	if err != nil {
		return handleError(err)
	}
	logger.Debug(ctx, "GetByID query: %s", query)
	row := c.db.QueryRowContext(ctx, query, record.ID())
	if row.Err() != nil {
		return handleError(row.Err())
	}
	return handleError(record.Scan(row))
}

func (c *MysqlDatabase) GetByFilter(ctx context.Context, filter *sql.Filter, records sql.Records, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var conditionValues []any
	var rows sql.Rows
	if opt.PreparedName != "" {
		var stmt *db.Stmt
		var ok bool

		if stmt, ok = c.preparedStatements[opt.PreparedName]; !ok {
			var query string
			query, conditionValues, err = parser.ParseGetByFilterQuery(filter, records)
			if err != nil {
				return handleError(err)
			}
			logger.Debug(ctx, "GetByFilter query: %s", query)
			stmt, err = c.db.PrepareContext(ctx, query)
			if err != nil {
				return handleError(err)
			}
			c.preparedStatements[opt.PreparedName] = stmt
		}
		rows, err = stmt.QueryContext(ctx, conditionValues...)
		if err != nil {
			return handleError(err)
		}
	} else {
		var query string
		query, conditionValues, err = parser.ParseGetByFilterQuery(filter, records)
		if err != nil {
			return handleError(err)
		}
		logger.Debug(ctx, "GetByFilter query: %s", query)
		rows, err = c.db.QueryContext(ctx, query, conditionValues...)
		if err != nil {
			return handleError(err)
		}
	}
	return handleError(records.ScanMany(rows))
}
