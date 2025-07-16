package mssql

import (
	"context"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *MssqlDatabase) Get(ctx context.Context, filter *sql.Filter, values []any, records sql.Records, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var filterIndexes []int
	var rows sql.Rows
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool

		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			var query string
			query, filterIndexes, err = parser.ParseGetByFilterQuery(filter, records)
			if err != nil {
				return internal.HandleError(err)
			}
			logger.Debug(ctx, "GetByFilter query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(filterIndexes)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		rows, err = stmt.GetStatement().QueryContext(ctx, sql.GetValues(stmt.GetValueIndexes(), values)...)
		if err != nil {
			return internal.HandleError(err)
		}
	} else {
		var query string
		query, filterIndexes, err = parser.ParseGetByFilterQuery(filter, records)
		if err != nil {
			return internal.HandleError(err)
		}
		logger.Debug(ctx, "GetByFilter query: %s", query)
		rows, err = c.db.QueryContext(ctx, query, sql.GetValues(filterIndexes, values)...)
		if err != nil {
			return internal.HandleError(err)
		}
	}
	return internal.HandleError(records.Scan(rows))
}

func (c *MssqlDatabase) GetByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool

		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, err := parser.ParseGetByIDQuery(record)
			if err != nil {
				return internal.HandleError(err)
			}
			logger.Debug(ctx, "GetByID query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}

		row := stmt.GetStatement().QueryRowContext(ctx, record.ID())
		if err = row.Err(); err != nil {
			return internal.HandleError(err)
		}
		return internal.HandleError(record.Scan(row))
	}
	query, err := parser.ParseGetByIDQuery(record)
	if err != nil {
		return internal.HandleError(err)
	}
	logger.Debug(ctx, "GetByID query: %s", query)
	row := c.db.QueryRowContext(ctx, query, record.ID())
	if row.Err() != nil {
		return internal.HandleError(row.Err())
	}
	return internal.HandleError(record.Scan(row))
}
