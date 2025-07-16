package mssql

import (
	"context"
	db "database/sql"
	"errors"
	"fmt"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
)

/*
Insert inserts a record into the database.
Returns an error if any.
It will set the ID of the record to the last inserted ID.
*/
func (c *MssqlDatabase) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	if record == nil {
		return sql.NewInvalidQueryError("record is nil")
	}

	opt := sql.GetOptions(options...)
	var err error
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		var query string
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, _, err = parser.ParseInsertQuery(record)
			if err != nil {
				return internal.HandleError(err)
			}
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps)
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}

		_, err = stmt.GetStatement().ExecContext(ctx, record.Values()...)
		if err != nil {
			return internal.HandleError(err)
		}
	} else {
		query, values, err := parser.ParseInsertQuery(record)
		if err != nil {
			return internal.HandleError(err)
		}
		_, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return internal.HandleError(err)
		}
	}

	// For MSSQL, we need to use OUTPUT clause to get the inserted ID
	// Execute a separate query to get the last inserted ID using OUTPUT
	idColumn := record.IdColumn()
	tableName := record.Table().Name
	outputQuery := fmt.Sprintf("SELECT TOP 1 %s FROM %s ORDER BY %s DESC", idColumn, tableName, idColumn)

	var id int64
	err = c.db.QueryRowContext(ctx, outputQuery).Scan(&id)
	if err != nil {
		return internal.HandleError(err)
	}
	record.SetID(id)
	return nil
}

/*
InsertMany inserts multiple records into the database.
Returns the number of rows affected and an error if any.
Returns 0, nil if no records are provided.
Returns 0, sql.ErrNoRecordInserted if no records are inserted.
Query will not be prepared because of variable length of records, if you want to prepare the query, use Insert instead.
*/
func (c *MssqlDatabase) InsertMany(ctx context.Context, records []sql.Record, options ...sql.Options) (int64, error) {
	// if no records to insert
	if len(records) == 0 {
		return 0, nil
	}
	opt := sql.GetOptions(options...)
	var err error
	var res db.Result
	if opt.PreparedName != "" {
		var stmt *internal.PreparedStatement
		var ok bool
		if stmt, ok = c.preparedStatements.Get(opt.PreparedName); !ok {
			query, _, err := parser.ParseInsertQuery(records...)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			ps, err := c.db.PrepareContext(ctx, query)
			if err != nil {
				return 0, internal.HandleError(err)
			}
			stmt = internal.NewPreparedStatement(ps).WithRecords(len(records))
			c.preparedStatements.Add(opt.PreparedName, stmt)
		}
		if stmt.GetNoOfRecords() != len(records) {
			return 0, errors.New("for insert many prepared statement, number of records should match with first inserted records")
		}
		res, err = stmt.GetStatement().ExecContext(ctx, getValues(records)...)
		if err != nil {
			return 0, internal.HandleError(err)
		}
	} else {
		query, values, err := parser.ParseInsertQuery(records...)
		if err != nil {
			return 0, internal.HandleError(err)
		}
		res, err = c.db.ExecContext(ctx, query, values...)
		if err != nil {
			return 0, internal.HandleError(err)
		}
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

func getValues(records []sql.Record) []any {
	values := make([]any, 0)
	for _, record := range records {
		values = append(values, record.Values()...)
	}
	return values
}
