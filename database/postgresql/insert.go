package postgresql

import (
	"context"
	"strconv"

	"database/sql"

	"github.com/gofreego/database/database/dbcommon"
	"github.com/gofreego/database/database/dberrors"

	"github.com/gofreego/goutils/logger"
)

/*
Insert a record into the postgresql database
*/
func (d *Database) Insert(ctx context.Context, record dbcommon.SQLRecord, options ...any) error {
	prepareName := dbcommon.GetPrepareName(options...)
	columns, values := record.InsertColumnsValues()
	var result *sql.Row
	var err error
	if prepareName != "" {
		stmt, ok := d.preparedStatements[prepareName]
		if !ok {
			// Prepare statement
			query := recordToInsertQuery(parseTableName(record.Table()), columns)
			logger.Debug(ctx, "Database::PostgreSQL::Insert::Query:%s: %s", prepareName, query)
			stmt, err = d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::Insert::Prepare statement failed for name %s, table %s , Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
		}
		// Execute statement
		result = stmt.QueryRowContext(ctx, values...)
	} else {
		query := recordToInsertQuery(parseTableName(record.Table()), columns)
		logger.Debug(ctx, "Database::PostgreSQL::Insert::Query: %s", query)
		result = d.conn.QueryRowContext(ctx, query, values...)
	}

	if err := result.Err(); err != nil {
		logger.Error(ctx, "Database::PostgreSQL::Insert::Insert failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("Insert failed for table "+parseTableName(record.Table()), err)
	}

	var id int64
	err = result.Scan(&id)
	if err != nil {
		return dberrors.ParseSQLError("LastInsertId failed for table "+parseTableName(record.Table()), err)
	}
	record.SetID(id)
	return nil
}

// InsertMany inserts multiple records of the same table into the database
func (d *Database) InsertMany(ctx context.Context, records []dbcommon.SQLRecord, options ...any) error {
	if len(records) == 0 {
		return nil
	}
	query, values := recordsToInsertManyQuery(records[0].Table(), records)
	logger.Debug(ctx, "InsertMany Query: %s", query)
	_, err := d.conn.ExecContext(ctx, query, values...)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::InsertMany::Insert failed for table %s, Err:%s", parseTableName(records[0].Table()), err.Error())
		return dberrors.ParseSQLError("Insert failed for table "+parseTableName(records[0].Table()), err)
	}
	return nil
}

/*
helper function to generate insert query
*/
func recordToInsertQuery(table string, columns []string) string {
	columnsStr := ""
	valuesStr := ""
	for i, column := range columns {
		columnsStr += column + ", "
		valuesStr += "$" + strconv.Itoa(i+1) + ", "
	}
	columnsStr = columnsStr[:len(columnsStr)-2]
	valuesStr = valuesStr[:len(valuesStr)-2]
	return "INSERT INTO " + table + " (" + columnsStr + ") VALUES (" + valuesStr + ") RETURNING id"
}

func recordsToInsertManyQuery(table *dbcommon.Table, records []dbcommon.SQLRecord) (string, []any) {
	columnsStr := ""
	valuesStr := ""
	valueNumber := 1
	allValues := []any{}
	for i, record := range records {
		valueStr := ""
		columns, values := record.InsertColumnsValues()

		for _, column := range columns {
			if i == 0 {
				columnsStr += column + ", "
			}
			valueStr += "$" + strconv.Itoa(valueNumber) + ", "
			valueNumber++
		}
		valuesStr = valuesStr + " ( " + valueStr[:len(valueStr)-2] + " ), "

		allValues = append(allValues, values...)
	}
	columnsStr = columnsStr[:len(columnsStr)-2]
	valuesStr = valuesStr[:len(valuesStr)-2]
	return "INSERT INTO " + parseTableName(table) + " (" + columnsStr + ") VALUES " + valuesStr, allValues
}
