package postgresql

import (
	"context"
	"database/sql"
	"openauth/database/database/dbcommon"
	"openauth/database/database/dberrors"
	"strconv"

	"github.com/gofreego/goutils/logger"
)

/*
Insert a record into the postgresql database
*/
func (d *Database) Insert(ctx context.Context, record dbcommon.Record, options ...any) error {
	prepareName := dbcommon.GetPrepareName(options...)
	columns, values := record.InsertColumnsValues()
	var result *sql.Row
	var err error
	if prepareName != "" {
		stmt, ok := d.preparedStatements[prepareName]
		if !ok {
			// Prepare statement
			query := recordToInsertQuery(record.TableName(), columns)
			logger.Debug(ctx, "Database::PostgreSQL::Insert::Query:%s: %s", prepareName, query)
			stmt, err = d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::Insert::Prepare statement failed for name %s, table %s , Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
		}
		// Execute statement
		result = stmt.QueryRowContext(ctx, values...)
	} else {
		result = d.conn.QueryRowContext(ctx, recordToInsertQuery(record.TableName(), columns), values...)
	}

	if err := result.Err(); err != nil {
		logger.Error(ctx, "Database::PostgreSQL::Insert::Insert failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("Insert failed for table "+record.TableName(), err)
	}

	var id int64
	err = result.Scan(&id)
	if err != nil {
		return dberrors.ParseSQLError("LastInsertId failed for table "+record.TableName(), err)
	}
	record.SetID(id)
	return nil
}

// InsertMany inserts multiple records of the same table into the database
func (d *Database) InsertMany(ctx context.Context, records []dbcommon.Record, options ...any) error {
	query, values := recordsToInsertManyQuery(records[0].TableName(), records)
	_, err := d.conn.ExecContext(ctx, query, values...)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::InsertMany::Insert failed for table %s, Err:%s", records[0].TableName(), err.Error())
		return dberrors.ParseSQLError("Insert failed for table "+records[0].TableName(), err)
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

func recordsToInsertManyQuery(table string, records []dbcommon.Record) (string, []any) {
	columnsStr := ""
	valuesStr := ""
	valueNumber := 1
	allValues := []any{}
	for _, record := range records {
		valueStr := ""
		columns, values := record.InsertColumnsValues()
		for _, column := range columns {
			columnsStr += column + ", "
			valuesStr += "$" + strconv.Itoa(valueNumber) + ", "
			valueNumber++
		}
		valuesStr = valuesStr + " ( " + valueStr[:len(valueStr)-2] + " ), "

		allValues = append(allValues, values)
	}
	columnsStr = columnsStr[:len(columnsStr)-2]
	valuesStr = valuesStr[:len(valuesStr)-2]
	return "INSERT INTO " + table + " (" + columnsStr + ") VALUES " + valuesStr, allValues
}
