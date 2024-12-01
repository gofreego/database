package postgresql

import (
	"context"
	"database/database/dbcommon"
	"database/database/dberrors"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) FindOneByID(ctx context.Context, record dbcommon.Record, options ...any) error {
	var row *sql.Row
	prepareName := dbcommon.GetPrepareName(options...)
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement

			stmt, err := d.conn.PrepareContext(ctx, generateFindOneByIDQuery(record.TableName(), record.SelectColumns()))
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::FindOneByID::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		row = d.preparedStatements[prepareName].QueryRowContext(ctx, record.ID())
	} else {
		row = d.conn.QueryRowContext(ctx, generateFindOneByIDQuery(record.TableName(), record.SelectColumns()), record.ID())
	}
	if row.Err() != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindOneByID::FindOneByID failed for table %s, Err:%s", record.TableName(), row.Err().Error())
		return dberrors.ParseSQLError("FindOneByID failed for table "+record.TableName(), row.Err())
	}
	err := record.ScanRow(row)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindOneByID::Scan failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("Scan failed for table "+record.TableName(), err)
	}
	return nil
}

func (d *Database) FindOneByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) error {
	var row *sql.Row
	prepareName := dbcommon.GetPrepareName(options...)
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			query, values := generateFindOneByFilterQuery(record.TableName(), record.SelectColumns(), filter)
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::FindOneByFilter::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
			// execute the statement
			row = stmt.QueryRowContext(ctx, values...)
		}
	} else {
		query, values := generateFindOneByFilterQuery(record.TableName(), record.SelectColumns(), filter)
		row = d.conn.QueryRowContext(ctx, query, values...)
	}
	if row.Err() != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindOneByFilter::FindOneByFilter failed for table %s, Err:%s", record.TableName(), row.Err().Error())
		return dberrors.ParseSQLError("FindOneByFilter failed for table "+record.TableName(), row.Err())
	}
	err := record.ScanRow(row)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindOneByFilter::Scan failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("Scan failed for table "+record.TableName(), err)
	}
	return nil
}

func (d *Database) FindAll(ctx context.Context, record dbcommon.Records, filter dbcommon.Filter, options ...any) error {
	var rows *sql.Rows
	var err error
	var values []interface{}
	var query string
	prepareName := dbcommon.GetPrepareName(options...)
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			query, values = generateFindAllQuery(record.TableName(), record.SelectColumns(), filter)
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::FindAll::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		rows, err = d.preparedStatements[prepareName].QueryContext(ctx, values...)
	} else {
		query, values = generateFindAllQuery(record.TableName(), record.SelectColumns(), filter)
		rows, err = d.conn.QueryContext(ctx, query, values...)
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindAll::FindAll failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("FindAll failed for table "+record.TableName(), err)
	}
	defer rows.Close()
	err = record.ScanRows(rows)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::FindAll::Scan failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("Scan failed for table "+record.TableName(), err)
	}
	return nil
}

/*
 helper function to generate query for find one by id
*/

func generateFindOneByIDQuery(tableName string, columns []string) string {
	return "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName + " WHERE id = $1"
}

/*
 helper function to generate query for find one by filter
*/

func generateFindOneByFilterQuery(tableName string, columns []string, filter dbcommon.Filter) (string, []interface{}) {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName
	var values []interface{}
	if filter != nil {
		valueNumber := 1
		condition, condValues := parseCondition(filter.Condition(), &valueNumber)
		if condition != "" {
			query += " WHERE " + condition
			values = append(values, condValues...)
		}
	}
	return query, values
}

/*

helper function to generate query for find all

*/

func generateFindAllQuery(tableName string, columns []string, filter dbcommon.Filter) (string, []interface{}) {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName
	var values []interface{}
	valueNumber := 1
	if filter != nil {
		condition, condValues := parseCondition(filter.Condition(), &valueNumber)
		if condition != "" {
			query += " WHERE " + condition
			values = append(values, condValues...)
		}
	}
	query += parseSort(filter.Sorts())
	if filter.Offset() > 0 {
		query += fmt.Sprintf(" OFFSET $%d", valueNumber)
		values = append(values, filter.Offset())
		valueNumber++
	}
	if filter.Limit() > 0 {
		query += fmt.Sprintf(" LIMIT $%d", valueNumber)
		values = append(values, filter.Limit())
		valueNumber++
	}
	return query, values
}
