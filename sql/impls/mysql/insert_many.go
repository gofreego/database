package mysql

import (
	"context"
	db "database/sql"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql/parser"
	"github.com/gofreego/database/sql/internal"
)

/*
InsertMany inserts multiple records into the database.
Returns the number of rows affected and an error if any.
Returns 0, nil if no records are provided.
Returns 0, sql.ErrNoRecordInserted if no records are inserted.
Query will not be prepared because of variable length of records, if you want to prepare the query, use Insert instead.
*/
func (c *MysqlDatabase) InsertMany(ctx context.Context, records []sql.Record, options ...sql.Options) (int64, error) {
	// if no records to insert
	if len(records) == 0 {
		return 0, nil
	}

	var err error
	var res db.Result
	query, values, err := parser.ParseInsertQuery(records...)
	if err != nil {
		return 0, internal.HandleError(err)
	}
	res, err = c.db.ExecContext(ctx, query, values...)
	if err != nil {
		return 0, internal.HandleError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, internal.HandleError(err)
	}
	if rowsAffected == 0 {
		return 0, sql.ErrNoRecordInserted
	}
	return rowsAffected, nil
}
