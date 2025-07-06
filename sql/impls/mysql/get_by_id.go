package mysql

import (
	"context"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *MysqlDatabase) GetByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
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
