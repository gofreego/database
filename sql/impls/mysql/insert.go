package mysql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
)

func (c *MysqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *db.Stmt
		var ok bool
		if stmt, ok = c.preparedStatements[opt.PreparedName]; !ok {
			stmt, err = c.db.PrepareContext(ctx, parser.ParseInsertQuery(record))
			if err != nil {
				return handleError(err)
			}
			c.preparedStatements[opt.PreparedName] = stmt
		}

		res, err = stmt.ExecContext(ctx, record.Values()...)
		if err != nil {
			return handleError(err)
		}
	} else {
		res, err = c.db.ExecContext(ctx, parser.ParseInsertQuery(record), record.Values()...)
		if err != nil {
			return handleError(err)
		}
	}
	id, err := res.LastInsertId()
	if err != nil {
		return handleError(err)
	}
	record.SetID(id)
	if err != nil {
		return handleError(err)
	}
	return nil
}

func getValues(records []sql.Record) []any {
	values := make([]any, 0)
	for _, record := range records {
		values = append(values, record.Values()...)
	}

	return values
}

func (c *MysqlDatabase) InsertMany(ctx context.Context, records []sql.Record, options ...sql.Options) (int64, error) {
	var err error
	var res db.Result
	res, err = c.db.ExecContext(ctx, parser.ParseInsertQuery(records...), getValues(records)...)
	if err != nil {
		return 0, handleError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, handleError(err)
	}
	if rowsAffected == 0 {
		return 0, sql.ErrNoRecordInserted
	}

	return rowsAffected, nil
}
