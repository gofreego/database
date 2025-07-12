package mysql

import (
	"context"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/goutils/logger"
)

func (c *MysqlDatabase) Get(ctx context.Context, filter *sql.Filter, values []any, records sql.Records, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var conditionValues []int
	var rows sql.Rows
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool

		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			var query string
			query, conditionValues, err = parser.ParseGetByFilterQuery(filter, records)
			if err != nil {
				return internal.HandleError(err)
			}
			logger.Debug(ctx, "GetByFilter query: %s", query)
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithValueIndexes(conditionValues)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		rows, err = stmt.GetStatement().QueryContext(ctx, sql.GetValues(stmt.GetValueIndexes(), values)...)
		if err != nil {
			return internal.HandleError(err)
		}
	} else {
		var query string
		query, conditionValues, err = parser.ParseGetByFilterQuery(filter, records)
		if err != nil {
			return internal.HandleError(err)
		}
		logger.Debug(ctx, "GetByFilter query: %s", query)
		rows, err = c.db.QueryContext(ctx, query, sql.GetValues(conditionValues, values)...)
		if err != nil {
			return internal.HandleError(err)
		}
	}
	return internal.HandleError(records.Scan(rows))
}
