package postgresql

import (
	"context"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
)

func (c *PostgresqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, _, err = c.parser.ParseInsertQuery(record)
			if err != nil {
				return internal.HandleError(err)
			}
			// Add RETURNING clause for PostgreSQL
			query += " RETURNING id"
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements[opt.PreparedName] = stmt
		}

		row := stmt.GetStatement().QueryRowContext(ctx, record.Values()...)
		if err = row.Err(); err != nil {
			return internal.HandleError(err)
		}
		var id int64
		if err = row.Scan(&id); err != nil {
			return internal.HandleError(err)
		}
		record.SetID(id)
	} else {
		query, values, err := c.parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		// Add RETURNING clause for PostgreSQL
		query += " RETURNING id"
		row := c.db.QueryRowContext(ctx, query, values...)
		if row.Err() != nil {
			return internal.HandleError(row.Err())
		}
		var id int64
		if err = row.Scan(&id); err != nil {
			return internal.HandleError(err)
		}
		record.SetID(id)
	}
	return nil
}
