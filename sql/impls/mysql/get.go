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
	var conditionValues []*sql.Value
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
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		// Convert sql.Value to actual values using the provided values slice
		actualValues := make([]any, len(conditionValues))
		for i, val := range conditionValues {
			if val.Index < len(values) {
				actualValues[i] = values[val.Index]
			}
		}
		rows, err = stmt.GetStatement().QueryContext(ctx, actualValues...)
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
		// Convert sql.Value to actual values using the provided values slice
		actualValues := make([]any, len(conditionValues))
		for i, val := range conditionValues {
			if val.Index < len(values) {
				actualValues[i] = values[val.Index]
			}
		}
		rows, err = c.db.QueryContext(ctx, query, actualValues...)
		if err != nil {
			return internal.HandleError(err)
		}
	}
	return internal.HandleError(records.Scan(rows))
}
