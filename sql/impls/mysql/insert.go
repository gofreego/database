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
	return nil
}

func getValues(records []sql.Record) []any {
	values := make([]any, 0)
	for _, record := range records {
		values = append(values, record.Values()...)
	}

	return values
}

/*
InsertMany inserts multiple records into the database.
Returns the number of rows affected and an error if any.
Returns 0, nil if no records are provided.
Returns 0, sql.ErrNoRecordInserted if no records are inserted.
Query will not be prepared, if you want to prepare the query, use Insert instead.
*/
func (c *MysqlDatabase) InsertMany(ctx context.Context, records []sql.Record, options ...sql.Options) (int64, error) {
	// if no records to insert
	if len(records) == 0 {
		return 0, nil
	}

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

func (c *MysqlDatabase) Upsert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *db.Stmt
		var ok bool
		if stmt, ok = c.preparedStatements[opt.PreparedName]; !ok {
			stmt, err = c.db.PrepareContext(ctx, parser.ParseUpsertQuery(record))
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
		res, err = c.db.ExecContext(ctx, parser.ParseUpsertQuery(record), record.Values()...)
		if err != nil {
			return handleError(err)
		}
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return handleError(err)
	}
	if rowsAffected == 0 {
		return sql.ErrNoRecordInserted
	}
	id, err := res.LastInsertId()
	if err != nil {
		return handleError(err)
	}
	record.SetID(id)
	return nil
}
