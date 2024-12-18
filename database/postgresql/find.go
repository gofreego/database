package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"openauth/database/database/dbcommon"
	"openauth/database/database/dberrors"
	"strings"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) FindOneByID(ctx context.Context, record dbcommon.Record, options ...any) error {
	var row *sql.Row
	prepareName := dbcommon.GetPrepareName(options...)
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			query := generateFindOneByIDQuery(record.TableName(), record.SelectColumns())
			logger.Debug(ctx, "Database::PostgreSQL::FindOneByID::Query:%s: %s", prepareName, query)
			stmt, err := d.conn.PrepareContext(ctx, query)
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
			query, values := generateFindQuery(record.TableName(), record.SelectColumns(), filter)
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::FindOneByFilter::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
			// execute the statement
			row = stmt.QueryRowContext(ctx, values...)
		} else {
			// execute the statement
			_, values := parseFilter(filter)
			row = d.preparedStatements[prepareName].QueryRowContext(ctx, values...)
		}
	} else {
		query, values := generateFindQuery(record.TableName(), record.SelectColumns(), filter)
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
			query, values = generateFindQuery(record.TableName(), record.SelectColumns(), filter)
			logger.Debug(ctx, "Database::PostgreSQL::FindAll::Query:%s: %s", prepareName, query)
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
		query, values = generateFindQuery(record.TableName(), record.SelectColumns(), filter)
		logger.Debug(ctx, "Database::PostgreSQL::FindAll::Query: %s, values: %v", query, values)
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

helper function to generate query for find all

*/

func generateFindQuery(tableName string, columns []string, filter dbcommon.Filter) (string, []interface{}) {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName
	condition, values := parseFilter(filter)
	logger.Debug(context.Background(), "%s", condition)
	return query + condition, values
}

func parseFilter(filter dbcommon.Filter) (string, []interface{}) {
	valueNumber := 1
	if filter != nil {
		condition, condValues := parseCondition(filter.Condition(), &valueNumber)
		if condition != "" {
			condition = " WHERE " + condition
		}

		condition += parseSort(filter.Sorts())
		condition += fmt.Sprintf(" OFFSET $%d", valueNumber)
		condValues = append(condValues, filter.Offset())
		valueNumber++

		if filter.Limit() > 0 {
			condition += fmt.Sprintf(" LIMIT $%d", valueNumber)
			condValues = append(condValues, filter.Limit())
			valueNumber++
		}
		return condition, condValues
	}
	return "", nil
}
